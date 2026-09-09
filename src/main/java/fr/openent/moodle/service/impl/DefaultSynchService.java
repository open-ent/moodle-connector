package fr.openent.moodle.service.impl;

import fr.openent.moodle.helper.HttpClientHelper;
import fr.openent.moodle.service.ModuleNeoRequestService;
import fr.openent.moodle.utils.SyncCase;
import fr.openent.moodle.utils.Utils;
import fr.wseduc.webutils.Either;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.entcore.common.neo4j.Neo4j;
import org.entcore.common.neo4j.Neo4jResult;
import org.entcore.common.user.UserUtils;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static fr.openent.moodle.Moodle.*;

public class DefaultSynchService {

    protected static final Logger log = LoggerFactory.getLogger(DefaultSynchService.class);
    private final Vertx vertx;
    protected EventBus eb;
    private final Neo4j neo4j = Neo4j.getInstance();

    private final ModuleNeoRequestService moduleNeoRequestService;

    private HttpClient httpClient;

    private String baseWsMoodleUrl;
    private JsonObject moodleClient;

    private Map<String, JsonObject> mapUsersMoodle;
    private Map<String, JsonObject> mapUsersFound;
    private Map<String, JsonObject>[] mapUsersNotFound;
    private JsonArray arrUsersToDelete = new JsonArray();
    private JsonArray arrUsersToEnroll = new JsonArray();

    private AtomicInteger compositeFuturEnded;

    public String acceptLanguage = "fr";

    public DefaultSynchService(EventBus eb, Vertx vertx) {
        this.eb = eb;
        this.vertx = vertx;
        this.moduleNeoRequestService = new DefaultModuleNeoRequestService();
    }

    private void initSyncUsers(JsonObject moodleClientToApply) {
        mapUsersMoodle = new HashMap<>();
        mapUsersFound = new HashMap<>();
        arrUsersToDelete = new JsonArray();
        arrUsersToEnroll = new JsonArray();
        mapUsersNotFound = new Map[]{new HashMap<String, JsonObject>()};
        compositeFuturEnded = new AtomicInteger(2);
        moodleClient = moodleClientToApply;
        baseWsMoodleUrl = (moodleClient.getString("address_moodle") + moodleClient.getString("ws-path"));
        httpClient = HttpClientHelper.createHttpClient(vertx, moodleClient);
    }


    private void putUsersInMap(Scanner scUsers) {
        while (scUsers.hasNextLine()) {
            String userLine = scUsers.nextLine();
            log.info(userLine);

            String[] values = userLine.split(",");

            try {
                JsonObject jsonUser = new JsonObject();
                jsonUser.put("id", values[2]);
                jsonUser.put("firstname", values[3]);
                jsonUser.put("lastname", values[4]);
                jsonUser.put("email", values[5]);
                mapUsersMoodle.put(values[2], jsonUser);
            } catch (Throwable t) {
                log.warn("Error reading user : " + userLine);
            }

        }
    }

    private void endSyncUsers (Handler<Either<String, JsonObject>> handler) {
        int nbcompositeFuturEnded = compositeFuturEnded.decrementAndGet();
        log.info("endSyncUsers : "+ nbcompositeFuturEnded);
        if (nbcompositeFuturEnded == 0) {
            httpClient.close();
            JsonObject result = new JsonObject();
            result.put("status", "ok");
            handler.handle(new Either.Right<>(result));
        }
    }

    public void syncUsers(Scanner scUsers, JsonObject moodleClient, Handler<Either<String, JsonObject>> handler) {
        log.info("START syncUsers");

        List<Future> listGetFuture = new ArrayList<>();
        initSyncUsers(moodleClient);
        putUsersInMap(scUsers);

        // ---------- HANDLERS --------------

        // ---------- delete users --------------

        Handler<Either<String, Buffer>> handlerDeleteUsers = event -> {
            if (event.isRight()) {
                log.info("END deleting users");
                endSyncUsers(handler);
            } else {
                httpClient.close();
                handler.handle(new Either.Left<>(event.left().getValue()));
                log.error("Error deleting users", event.left());
            }
        };
        // ---------- END delete users --------------

        // ---------- enrolls users --------------
        Handler<Either<String, Buffer>> handlerEnrollUsers = event -> {
            if (event.isRight()) {
                log.info("END enrolling user individually");
                if (arrUsersToDelete.isEmpty()) {
                    String message = "Aucune suppression necessaire";
                    log.info(message);
                    endSyncUsers(handler);
                } else {
                    // Un fois inscriptions individuelles ok, lancer les suppressions
                    try {
                        deleteUsers(arrUsersToDelete, handlerDeleteUsers);
                    } catch (UnsupportedEncodingException e) {
                        httpClient.close();
                        handler.handle(new Either.Left<>(e.toString()));
                        log.error("Error deleteUsers - UnsupportedEncodingException", e);
                    }
                }
            } else {
                httpClient.close();
                handler.handle(new Either.Left<>(event.left().getValue()));
                log.error("Error enrolling user individually", event.left());
            }

        };
        // ---------- END enrolls users --------------

        // ---------- getCourses for each user deleted --------------
        Handler<AsyncResult<CompositeFuture>> handlerGetCourses = eventFuture -> {

            if (eventFuture.succeeded()) {
                log.info("END getting courses");

                // Utilisateurs non retrouvés (supp physiquement)
                for (Map.Entry<String, JsonObject> entryUser : mapUsersNotFound[0].entrySet()) {
                    JsonObject jsonUser = entryUser.getValue();
                    JsonArray jsonArrayCourses = mapUsersMoodle.get(jsonUser.getString("id")).getJsonArray("courses");

                    identifyUserToDeleteAndEnroll(arrUsersToDelete, arrUsersToEnroll, jsonArrayCourses,
                            jsonUser.getString("id"), SyncCase.SYNC_USER_NOT_FOUND);

                }

                // Utilisateurs en instance de suppresion
                for (Map.Entry<String, JsonObject> entryUser : mapUsersFound.entrySet()) {
                    JsonObject jsonUser = entryUser.getValue();
                    if (jsonUser.getValue("deleteDate") == null) {
                        continue;
                    }
                    JsonArray jsonArrayCourses = mapUsersMoodle.get(jsonUser.getString("id")).getJsonArray("courses");

                    identifyUserToDeleteAndEnroll(arrUsersToDelete, arrUsersToEnroll, jsonArrayCourses,
                            jsonUser.getString("id"), SyncCase.SYNC_USER_FOUND);
                }

                if (arrUsersToEnroll.isEmpty()) {
                    String message = "Aucune inscription individuelle necessaire";
                    log.info(message);
                    endSyncUsers(handler);
                } else {
                    try {
                        enrollUsersIndivudually(arrUsersToEnroll, handlerEnrollUsers);
                    } catch (UnsupportedEncodingException e) {
                        httpClient.close();
                        handler.handle(new Either.Left<>(e.toString()));
                        log.error("Error enrollUsersIndivudually - UnsupportedEncodingException", e);
                    }
                }
            } else {
                httpClient.close();
                handler.handle(new Either.Left<>("Error getting users Future"));
                log.error("Error getting users Future", eventFuture.cause());
            }

        };
        // ---------- END getCourses for each user deleted --------------


        // ---------- getUsers --------------
        final Handler<Either<String, JsonArray>> getUsersHandler = resultUsers -> {
            try {
                if (resultUsers.isLeft()) {
                    httpClient.close();
                    handler.handle(new Either.Left<>("Error getting users Future"));
                    log.error("Error getting users Future", resultUsers.left());
                } else {
                    log.info("End getting users");
                    JsonArray usersFromNeo = resultUsers.right().getValue();
                    for (Object userFromNeo : usersFromNeo) {
                        JsonObject jsonUserFromNeo = (JsonObject) userFromNeo;
                        mapUsersFound.put(jsonUserFromNeo.getString("id"), jsonUserFromNeo);
                    }
                    mapUsersNotFound[0] = new HashMap<>(mapUsersMoodle);
                    mapUsersNotFound[0].keySet().removeAll(mapUsersFound.keySet());
                    JsonArray arrUsersToUpdate = new JsonArray();
                    for (Map.Entry<String, JsonObject> entryUser : mapUsersFound.entrySet()) {
                        JsonObject jsonUserFromNeo = entryUser.getValue();
                        if (jsonUserFromNeo.getValue("deleteDate") != null) {
                            listGetFuture.add(getCourses(jsonUserFromNeo));
                        }
                         if (!areUsersEquals(jsonUserFromNeo, mapUsersMoodle.get(jsonUserFromNeo.getString("id")))) {
                             jsonUserFromNeo.put("email", jsonUserFromNeo.getString("id") + "@moodle.net");
                             jsonUserFromNeo.put("username", jsonUserFromNeo.getString("id"));
                             arrUsersToUpdate.add(jsonUserFromNeo);
                         }
                    }
                    for (Map.Entry<String, JsonObject> entryUser : mapUsersNotFound[0].entrySet()) {
                        JsonObject jsonUserFromNeo = entryUser.getValue();
                        listGetFuture.add(getCourses(jsonUserFromNeo));

                    }
                    if (arrUsersToUpdate.isEmpty()) {
                        String message = "Aucune mise à jour necessaire";
                        log.info(message);
                        endSyncUsers(handler);
                    } else {
                        try {
                            updateUsers(arrUsersToUpdate, event -> {
                                if (event.isRight()) {
                                    log.info("END updating users");
                                    endSyncUsers(handler);
                                } else {
                                    httpClient.close();
                                    handler.handle(new Either.Left<>(event.left().getValue()));
                                    log.error("Error updating users", event.left());
                                }
                            });
                        } catch (UnsupportedEncodingException e) {
                            httpClient.close();
                            handler.handle(new Either.Left<>(e.toString()));
                            log.error("Error updating users - UnsupportedEncodingException", e);
                        }
                    }
                    if (listGetFuture.isEmpty()) {
                        String message = "Aucun utilisateur supprimé avec des cours";
                        log.info(message);
                        endSyncUsers(handler);
                    } else {
                        Future.all((List)listGetFuture).onComplete(handlerGetCourses);
                    }
                }
            } catch (Throwable t) {
                log.error("Erreur getUsersHandler : ", t);
            }
        };
        // ---------- END getUsers --------------

        // ---------- END HANDLERS --------------

        getUsers (mapUsersMoodle.keySet().toArray(), getUsersHandler);

    }


    private void identifyUserToDelete (JsonArray arrUsersToDelete, String idUser, boolean remove) {
        boolean alreadyInList = arrUsersToDelete.stream().anyMatch(u -> ((JsonObject)u).getString("id").equals(idUser));
        if(alreadyInList){
            for(int i = 0; i< arrUsersToDelete.size(); i++){
                if(arrUsersToDelete.getJsonObject(i).getString("id").equals(idUser) && remove){
                    arrUsersToDelete.getJsonObject(i).put("remove", "hard");
                }
            }
        }else {
            JsonObject userToDelete = new JsonObject();
            userToDelete.put("id", idUser).put("remove", remove ? "hard" : "soft");
            arrUsersToDelete.add(userToDelete);
        }
    }

    private void identifyUserToEnroll (JsonArray arrCoursesUserToEnroll, String coursId, String role) {
        // Inscription en tant qu'apprenant individuel au cours :
        JsonObject courseToEnroll = new JsonObject();
        courseToEnroll.put("id", coursId);
        courseToEnroll.put("role", role);
        arrCoursesUserToEnroll.add(courseToEnroll);
    }

    private void identifyUserToDeleteAndEnroll(JsonArray arrUsersToDelete, JsonArray arrUsersToEnroll, JsonArray jsonArrayCourses,
                                               String idUser, SyncCase syncCase) {


        if (jsonArrayCourses != null) {
            JsonObject userToEnroll = new JsonObject();
            userToEnroll.put("id", idUser);

            JsonArray arrCoursesUserToEnroll = new JsonArray();


            if(syncCase.equals(SyncCase.SYNC_USER_FOUND)) {
                if (!jsonArrayCourses.isEmpty()) {
                    for (Object cours : jsonArrayCourses) {
                        JsonObject jsonCours = ((JsonObject) cours);

                        // si professeur (propriétaire ou éditeur) il doit toujours avoir acces à ses cours
                        if (jsonCours.getString("role").equals(ROLE_EDITEUR.toString())) {
                            // Inscription en tant qu'apprenant individuel au cours :
                            identifyUserToEnroll(arrCoursesUserToEnroll, jsonCours.getInteger("courseid").toString(), ROLE_EDITEUR.toString());
                        }

                        // si eleve (apprenant)
                        if (jsonCours.getString("role").equals(ROLE_APPRENANT.toString())) {
                            // Inscription en tant qu'apprenant individuel au cours :
                            identifyUserToEnroll(arrCoursesUserToEnroll, jsonCours.getInteger("courseid").toString(), ROLE_APPRENANT.toString());

                            // Détachement de l’utilisateur de ses cohortes sans le supprimer physiquement
                            identifyUserToDelete(arrUsersToDelete, idUser, false);
                        }
                    }
                }
            } else if(syncCase.equals(SyncCase.SYNC_USER_NOT_FOUND)) {

                if (!jsonArrayCourses.isEmpty()) {
                    for (Object cours : jsonArrayCourses) {
                        JsonObject jsonCours = ((JsonObject) cours);

                        // si auteur
                        if (jsonCours.getString("role").equals(ROLE_AUDITEUR.toString())) {
                            // TODO réattribuer le cours à un admin étab CGI (A préciser)
                            //    récupérer l'ADML dans l'ENT / le créer dans Moodle si besoin
                            //    utiliser web service Moodle  local_entcgi_services_enrolluserscourses pour inscrire l'ADML en tant que propriétaire du cours à la place de l'ancien

                            // Suppression de l’utilisateur (ou rendre inactif si trop coûteux en perf et purge à posteriori) + détachement de ses cohortes :
                            identifyUserToDelete(arrUsersToDelete, idUser, true);
                        }

                        // si editeur ou apprenant
                        if (jsonCours.getString("role").equals(ROLE_EDITEUR.toString())
                                || jsonCours.getString("role").equals(ROLE_APPRENANT.toString())) {

                            // Détachement de l’utilisateur de ses cohortes + supprimer physiquement ?
                            // --> suppression soft dans un premier temps, et purge par la suite

                            getUsersEnrolmentsFromMoodle(jsonCours.getInteger("courseid")).onComplete(event -> {
                                if (event.succeeded()) {
                                    int nbEditeur = 0;
                                    boolean isEditor = false;
                                    JsonArray usersEnrolment = event.result();
                                    JsonArray users = usersEnrolment.getJsonObject(0).getJsonArray("enrolments").getJsonObject(0).getJsonArray("users");
                                    for (int i = 0; i < users.size(); i++) {
                                        JsonObject user = users.getJsonObject(i);
                                        if (user.getInteger("role").equals(ROLE_EDITEUR)) {
                                            nbEditeur++;
                                            isEditor = user.getString("id").equals(idUser);
                                        }
                                    }
                                    identifyUserToDelete(arrUsersToDelete, idUser, nbEditeur == 1 && isEditor);
                                }
                            });
                        }
                    }
                }
            } else if(syncCase.equals(SyncCase.SYNC_GROUP)) {
                if (!jsonArrayCourses.isEmpty()) {
                    for (Object cours : jsonArrayCourses) {
                        JsonObject jsonCours = ((JsonObject) cours);
                        // Inscription en tant qu'apprenant individuel au cours :
                        identifyUserToEnroll(arrCoursesUserToEnroll, jsonCours.getInteger("courseid").toString(),
                                jsonCours.getString("role"));
                    }
                }
            }

            userToEnroll.put("courses", arrCoursesUserToEnroll);
            arrUsersToEnroll.add(userToEnroll);
        }
    }

    private void deleteUsers(JsonArray arrUsersToDelete, Handler<Either<String, Buffer>> handlerDeleteUserse) throws UnsupportedEncodingException {
        log.info("START deleting users");
        JsonObject body = new JsonObject();
        body.put("parameters", arrUsersToDelete)
                .put("wstoken", moodleClient.getString("wsToken"))
                .put("wsfunction", WS_POST_DELETE_USER)
                .put("moodlewsrestformat", JSON);;
        HttpClientHelper.webServiceMoodlePost(body, baseWsMoodleUrl, vertx, moodleClient, handlerDeleteUserse);
    }

    private void updateUsers(JsonArray arrUsersToUpdate, Handler<Either<String, Buffer>> handlerUpdateUser) throws UnsupportedEncodingException {
        log.info("START updating users");
        JsonObject body = new JsonObject();
        body.put("parameters", arrUsersToUpdate)
                .put("wstoken", moodleClient.getString("wsToken"))
                .put("wsfunction", WS_POST_CREATE_OR_UPDATE_USER)
                .put("moodlewsrestformat", JSON);
        HttpClientHelper.webServiceMoodlePost(body, baseWsMoodleUrl, vertx, moodleClient, handlerUpdateUser);
    }

    private void enrollUsersIndivudually(JsonArray arrUsersToEnroll, Handler<Either<String, Buffer>> handlerEnrollUsers) throws UnsupportedEncodingException {
        log.info("START enrolling users individually");
        JsonObject body = new JsonObject();
        body.put("parameters", arrUsersToEnroll)
                .put("wstoken", moodleClient.getString("wsToken"))
                .put("wsfunction", WS_POST_ENROLL_USERS_COURSES)
                .put("moodlewsrestformat", JSON);
        HttpClientHelper.webServiceMoodlePost(body, baseWsMoodleUrl, vertx, moodleClient, handlerEnrollUsers);
    }

    private Future<JsonArray> getUsersEnrolmentsFromMoodle(Integer idCourse) {
        final Promise<JsonArray> promise = Promise.promise();
        final HttpClient httpClient = HttpClientHelper.createHttpClient(vertx, moodleClient);
        final AtomicBoolean responseIsSent = new AtomicBoolean(false);
        Buffer wsResponse = new BufferImpl();
        final String moodleUrl = baseWsMoodleUrl +
                "?wstoken=" + moodleClient.getString("wsToken") +
                "&wsfunction=" + WS_GET_SHARECOURSE +
                "&parameters[courseid]=" + idCourse +
                "&moodlewsrestformat=" + JSON;
        Handler<HttpClientResponse> getUsersEnrolmentsHandler = response -> {
            if (response.statusCode() == 200) {
                response.handler(wsResponse::appendBuffer);
                response.endHandler(end -> {
                    JsonArray finalGroups = new JsonArray(wsResponse);
                    promise.complete(finalGroups);
                    if (!responseIsSent.getAndSet(true)) {
                        httpClient.close();
                    }
                });
            } else {
                log.error("Fail to call get share course right webservice" + response.statusMessage());
                response.bodyHandler(event -> {
                    log.error("Returning body after GET CALL : " + moodleUrl + ", Returning body : " + event.toString("UTF-8"));
                    promise.fail(response.statusMessage());
                    if (!responseIsSent.getAndSet(true)) {
                        httpClient.close();
                    }
                });
            }
        };
        httpClient.request(new RequestOptions()
                .setMethod(HttpMethod.GET)
                .setAbsoluteURI(moodleUrl)
                .addHeader("Content-Length", "0"))
                .flatMap(HttpClientRequest::send)
                .onSuccess(getUsersEnrolmentsHandler)
                .onFailure(event -> {
                    //Typically an unresolved Address, a timeout about connection or response
                    log.error(event.getMessage(), event);
                    promise.fail(event.getMessage());
                    if (!responseIsSent.getAndSet(true)) {
                        httpClient.close();
                    }
                });
        return promise.future();
    }

    private Future getCourses(JsonObject jsonUser) {
        final Promise promise = Promise.promise();
        final String moodleUrl = baseWsMoodleUrl + "?wstoken=" + moodleClient.getString("wsToken") +
                "&wsfunction=" + WS_GET_USERCOURSES +
                "&parameters[userid]=" + jsonUser.getString("id") +
                "&moodlewsrestformat=" + JSON;
        final AtomicBoolean responseIsSent = new AtomicBoolean(false);
        Buffer wsResponse = new BufferImpl();
        log.info("Start retrieving courses for user " + jsonUser.getString("id"));
        httpClient.request(new RequestOptions()
                .setMethod(HttpMethod.GET)
                .setAbsoluteURI(moodleUrl)
                .addHeader("Content-Length", "0"))
                .flatMap(HttpClientRequest::send)
                .onSuccess(response -> {
                    if (response.statusCode() == 200) {

                        response.handler(wsResponse::appendBuffer);
                        response.endHandler(end -> {
                            JsonArray object = new JsonArray(wsResponse);
                            JsonArray userCoursesDuplicate = object.getJsonObject(0).getJsonArray("enrolments");
                            JsonArray userCourses = Utils.removeDuplicateCourses(userCoursesDuplicate);

                            jsonUser.put("courses", userCourses);
                            mapUsersMoodle.put(jsonUser.getString("id"), jsonUser);
                            log.info("End retrieving courses for user " + jsonUser.getString("id"));
                            promise.complete();
                        });
                    } else {
                        log.error("Error retrieving courses for user " + jsonUser.getString("id"));
                        log.error("response.statusCode() = " + response.statusCode());
                        promise.complete();
                    }
                })
                .onFailure(event -> {
                    //Typically an unresolved Address, a timeout about connection or response
                    log.error(event.getMessage(), event);
                    responseIsSent.getAndSet(true);//renderError(request);
                });
        return promise.future();
    }

    private boolean areUsersEquals(JsonObject jsonUserFromNeo, JsonObject jsonUserFromMoodle) {
        return jsonUserFromMoodle.getString("id").equals(jsonUserFromNeo.getString("id")) &&
                jsonUserFromMoodle.getString("firstname").equals(jsonUserFromNeo.getString("firstname")) &&
                jsonUserFromMoodle.getString("lastname").equals(jsonUserFromNeo.getString("lastname"));
    }


    private void getUsers(Object[] idUsers, Handler<Either<String, JsonArray>> handler){

        String RETURNING = " RETURN  u.id as id, u.firstName as firstname, u.lastName as lastname, u.login as username, " +
                "u.deleteDate as deleteDate ORDER BY lastname, firstname";

        JsonObject params = new JsonObject();
        params.put("idUsers", new fr.wseduc.webutils.collections.JsonArray(Arrays.asList(idUsers)));

        String query = " MATCH (u:User) WHERE u.id IN {idUsers} " +
                RETURNING;
        neo4j.execute(query,params, Neo4jResult.validResultHandler(handler));

    }


    // --------------------------------------- GROUPS ---------------------------------------------------
    private void updateCohorts(JsonArray arrCohortsToUpdate, Handler<Either<String, Buffer>> handlerCohort) throws UnsupportedEncodingException {
        log.info("START updating cohorts");
        JsonObject body = new JsonObject();
        body.put("parameters", arrCohortsToUpdate)
                .put("wstoken", moodleClient.getString("wsToken"))
                .put("wsfunction", WS_POST_UPDATE_COHORTS)
                .put("moodlewsrestformat", JSON);
        HttpClientHelper.webServiceMoodlePost(body, baseWsMoodleUrl, vertx, moodleClient, handlerCohort);
    }

    private void deleteCohorts(JsonArray arrCohortsToDelete, Handler<Either<String, Buffer>> handlerCohort) throws UnsupportedEncodingException {
        log.info("START deleting cohorts");
        JsonObject body = new JsonObject();
        body.put("parameters", arrCohortsToDelete)
                .put("wstoken", moodleClient.getString("wsToken"))
                .put("wsfunction", WS_POST_DELETE_COHORTS)
                .put("moodlewsrestformat", JSON);
        HttpClientHelper.webServiceMoodlePost(body, baseWsMoodleUrl, vertx, moodleClient, handlerCohort);
    }



    private Map<String, JsonObject> mapCohortsMoodle;
    private Map<String, JsonObject> mapCohortsFound;
    private Map<String, JsonObject> mapCohortsNotFound;
    private JsonArray arrCohortsToDelete;
    private JsonArray arrCohortsToUpdate;

    private void initSyncGroups(JsonObject moodleClientToApply) {
        mapCohortsMoodle = new HashMap<>();
        mapCohortsFound = new HashMap<>();
        mapCohortsNotFound = new HashMap<>();

        mapUsersMoodle = new HashMap<>();

        arrUsersToDelete = new JsonArray();
        arrUsersToEnroll = new JsonArray();

        arrCohortsToDelete = new JsonArray();
        arrCohortsToUpdate = new JsonArray();

        compositeFuturEnded = new AtomicInteger(3);

        moodleClient = moodleClientToApply;
        baseWsMoodleUrl = (moodleClient.getString("address_moodle") + moodleClient.getString("ws-path"));

        httpClient = HttpClientHelper.createHttpClient(vertx, moodleClient);
    }


    private void putCohortsInMap(JsonArray jsonArrayCohorts) {
        for (Object objChorts : jsonArrayCohorts) {
            JsonObject jsonCohort = ((JsonObject)objChorts);
            log.info(jsonCohort);
            try {
                // suppression prefixe
                String idCohort = jsonCohort.getString("idnumber");
                /*idCohort = idCohort.replace("GR_","");
                idCohort = idCohort.replace("SB","");*/

                // stockage par id (id ENT classe/groupe/sharebookmark)
                mapCohortsMoodle.put(idCohort, jsonCohort);
            } catch (Throwable t) {
                log.warn("Error reading cohort : " + jsonCohort, t);
            }

        }
    }


    private void endSyncGroups (Handler<Either<String, JsonObject>> handler) {
        httpClient.close();
        JsonObject result = new JsonObject();
        result.put("status", "ok");
        handler.handle(new Either.Right<>(result));
    }

    private Handler<Either<String, Buffer>> handlerEnrollGroups;
    private Handler<AsyncResult<CompositeFuture>> handlerUpdateAndDeleteCohorts;

    public void syncGroups(JsonArray jsonArrayCohorts, JsonObject moodleClient, Handler<Either<String, JsonObject>> handler) {
        log.info("START syncGroups");
        initSyncGroups(moodleClient);
        putCohortsInMap(jsonArrayCohorts);

        //---HANDLERS--
        final Promise getGroupsFuture = Promise.promise();
        Handler<Either<String, JsonArray>> getGroupsHandler = resultGroups -> {
            try {
                if (resultGroups.isLeft()) {
                    httpClient.close();
                    handler.handle(new Either.Left<>("Error getting groups"));
                    log.error("Error getting groups", resultGroups.left());
                    getGroupsFuture.fail("Error getting groups");
                } else {
                    log.info("End getting groups");
                    JsonArray groupsFromNeo = resultGroups.right().getValue();
                    for (Object objGroup : groupsFromNeo) {
                        JsonObject jsonGroup = ((JsonObject) objGroup);

                        // suppression prefix
                        String idGroup = jsonGroup.getString("id");
                        //idGroup.replace("GR_","");
                        jsonGroup.put("id", idGroup);
                        mapCohortsFound.put(idGroup, jsonGroup);
                    }
                    getGroupsFuture.complete();
                }
            } catch (Throwable t) {
                log.error("Erreur getGroupsHandler : ", t);
                getGroupsFuture.fail("Error getting groups");
            }
        };

        Promise getSharedBookMarkFuture = Promise.promise();
        Handler<Either<String, Map<String, JsonObject>>> getSharedBookMarkHandler = resultSharedBookMark -> {
            try {
                if (resultSharedBookMark.isLeft()) {
                    httpClient.close();
                    handler.handle(new Either.Left<>("Error getting bookmarks"));
                    log.error("Error getting groups", resultSharedBookMark.left());
                    getSharedBookMarkFuture.fail("Error getting bookmarks");
                } else {
                    log.info("End getting bookmarks");
                    Map<String, JsonObject> mapShareBookMarks = resultSharedBookMark.right().getValue();
                    if (mapShareBookMarks != null && !mapShareBookMarks.isEmpty()) {
                        mapCohortsFound.putAll(mapShareBookMarks);
                    }
                    getSharedBookMarkFuture.complete();
                }
            } catch (Throwable t) {
                log.error("Erreur getSharedBookMarkHandler : ", t);
                getSharedBookMarkFuture.fail("Error getting bookmarks");
            }
        };

        Handler<AsyncResult<CompositeFuture>> handlerGetAllGroups = compositeFutureAsyncResult -> {
            if (compositeFutureAsyncResult.failed()) {
                httpClient.close();
                handler.handle(new Either.Left<>("Error getting all groups Future"));
                log.error("Error getting all groups Future", compositeFutureAsyncResult.cause());
            } else {

                // groupes / sharebook non retrouvés dans l'ENT
                mapCohortsNotFound = new HashMap<>(mapCohortsMoodle);
                mapCohortsNotFound.keySet().removeAll(mapCohortsFound.keySet());

                JsonArray usersIdsToEnrrollIndivually = new JsonArray();

                // Cohortes NON retrouvées ->
                for (Map.Entry<String, JsonObject> entryCohort : mapCohortsNotFound.entrySet()) {
                    JsonObject jsonCohort = entryCohort.getValue();
                    JsonObject jsonMoodleCohort = mapCohortsMoodle.get(jsonCohort.getString("idnumber"));

                    JsonArray moodleUsers = jsonMoodleCohort.getJsonArray("users");
                    if (moodleUsers != null && !moodleUsers.isEmpty()) {
                        usersIdsToEnrrollIndivually.addAll(moodleUsers);
                    }

                    JsonObject cohortToDelete = new JsonObject().put("id", jsonCohort.getString("idnumber"));
                    arrCohortsToDelete.add(cohortToDelete);
                }


                // Cohortes retrouvées ->
                for (Map.Entry<String, JsonObject> entryCohort : mapCohortsFound.entrySet()) {
                    JsonObject jsonCohortNeo = entryCohort.getValue();
                    JsonArray arrUsersNeo = jsonCohortNeo.getJsonArray("users");

                    JsonObject jsonMoodleCohort = mapCohortsMoodle.get(jsonCohortNeo.getString("id"));
                    JsonArray arrUsersMoodle = jsonMoodleCohort.getJsonArray("users");

                    // si plus aucun membre : suppression de la cohorte
                    if (arrUsersNeo == null || arrUsersNeo.isEmpty()) {

                        JsonArray moodleUsers = jsonMoodleCohort.getJsonArray("users");
                        if (moodleUsers != null && !moodleUsers.isEmpty()) {
                            usersIdsToEnrrollIndivually.addAll(moodleUsers);
                        }

                        JsonObject cohortToDelete = new JsonObject().put("id", jsonCohortNeo.getString("id"));
                        arrCohortsToDelete.add(cohortToDelete);
                    } else {

                        // identification changements
                        UserUtils.groupDisplayName(jsonCohortNeo, acceptLanguage);
                        boolean hasChangeName = !jsonCohortNeo.getString("name").equals(jsonMoodleCohort.getString("name"));
                        final JsonObject[] jsonCohorteWithUpdate = {null};
                        if (hasChangeName) {
                            jsonCohorteWithUpdate[0] = new JsonObject();
                            jsonCohorteWithUpdate[0].put("id", jsonCohortNeo.getString("id"));
                            jsonCohorteWithUpdate[0].put("newname", jsonCohortNeo.getString("name"));
                        }

                        // identification des nouveaux utilisateurs dans la cohorte
                        for (Object objUserNeo : arrUsersNeo) {
                            JsonObject jsonUserNeo = ((JsonObject) objUserNeo);
                            boolean exist = arrUsersMoodle.stream().anyMatch(u -> u.equals(jsonUserNeo.getString("id")));
                            if (!exist) {
                                if (jsonCohorteWithUpdate[0] == null) {
                                    jsonCohorteWithUpdate[0] = new JsonObject();
                                    jsonCohorteWithUpdate[0].put("id", jsonCohortNeo.getString("id"));
                                    jsonCohorteWithUpdate[0].put("newname", jsonCohortNeo.getString("name"));
                                }
                                JsonArray useradded = jsonCohorteWithUpdate[0].getJsonArray("useradded");

                                if (useradded == null) {
                                    useradded = new JsonArray();
                                }

                                jsonUserNeo.put("email", jsonUserNeo.getString("id") + "@moodle.net");
                                useradded.add(jsonUserNeo);
                                jsonCohorteWithUpdate[0].put("useradded", useradded);
                            }
                        }

                        // identification des utilisateurs supprimés de la cohorte
                        // pour ces utilisateurs on les inscriras individuellement à leurs cours dans la suite
                        //de l'algo
                        for (Object objUserMoodle : arrUsersMoodle) {
                            String idUserMoodle = ((String) objUserMoodle);
                            boolean exist = arrUsersNeo.stream().anyMatch(u -> ((JsonObject) u).getString("id").equals(idUserMoodle));
                            if (!exist) {
                                if (jsonCohorteWithUpdate[0] == null) {
                                    jsonCohorteWithUpdate[0] = new JsonObject();
                                    jsonCohorteWithUpdate[0].put("id", jsonCohortNeo.getString("id"));
                                    jsonCohorteWithUpdate[0].put("newname", jsonCohortNeo.getString("name"));
                                }
                                JsonArray userdeleted = jsonCohorteWithUpdate[0].getJsonArray("userdeleted");

                                if (userdeleted == null) {
                                    userdeleted = new JsonArray();
                                }

                                JsonObject jsonUserMoodle = new JsonObject();
                                jsonUserMoodle.put("id", idUserMoodle);
                                userdeleted.add(jsonUserMoodle);
                                jsonCohorteWithUpdate[0].put("userdeleted", userdeleted);

                                usersIdsToEnrrollIndivually.add(jsonUserMoodle.getString("id"));
                            }
                        }

                        if (jsonCohorteWithUpdate[0] != null) {
                            arrCohortsToUpdate.add(jsonCohorteWithUpdate[0]);
                        }
                    }
                }

                // Inscription individuel de tous les utilisateurs à leurs cours
                // s'ils sont identifiés comme sortant d'une cohorte
                try {
                    getUsersCoursesAndEnroll(usersIdsToEnrrollIndivually, handler);
                } catch (UnsupportedEncodingException e) {
                    httpClient.close();
                    handler.handle(new Either.Left<>(e.toString()));
                    log.error("Error getUsersCoursesAndEnroll - UnsupportedEncodingException", e);
                }
            }
        };

        handlerEnrollGroups = event -> {
            if (event.isRight()) {
                log.info("END enrolling users individually");
                try {
                    updateAnDeleteCohorts(handler);
                } catch (UnsupportedEncodingException e) {
                    httpClient.close();
                    handler.handle(new Either.Left<>(e.toString()));
                    log.error("Error updateAnDeleteCohorts - UnsupportedEncodingException", e);
                }
            } else {
                httpClient.close();
                handler.handle(new Either.Left<>(event.left().getValue()));
                log.error("Error enrolling users individually", event.left());
            }

        };


        handlerUpdateAndDeleteCohorts = resultUpdateAndDelete -> {
            if (resultUpdateAndDelete.failed()) {
                httpClient.close();
                handler.handle(new Either.Left<>("Error update/delete cohort Future"));
                log.error("Error update/delete cohort Future", resultUpdateAndDelete.cause());
            } else {
                endSyncGroups(handler);
            }
        };
        //---FIN HANDLERS--


        Future.all(getGroupsFuture.future(), getSharedBookMarkFuture.future()).onComplete(handlerGetAllGroups);


        List lstGroupIds = Arrays.asList(mapCohortsMoodle.keySet().toArray());
        lstGroupIds = (List) lstGroupIds.stream().map(
                o -> ((String) o).replace("SB", "").replace("GR_", "")).collect(Collectors.toList());

        JsonArray groupsIds = new JsonArray(lstGroupIds);
        moduleNeoRequestService.getGroups(groupsIds, getGroupsHandler);
        getDistinctSharedBookMarkUsers(groupsIds, getSharedBookMarkHandler);
    }

    private void getDistinctSharedBookMarkUsers(final JsonArray bookmarksIds, Handler<Either<String, Map<String, JsonObject>>> handler) {
        moduleNeoRequestService.getSharedBookMarkUsers(bookmarksIds, resultSharedBookMark -> {
            if (resultSharedBookMark.isLeft()) {
                log.error("Error getting getSharedBookMarkUsers", resultSharedBookMark.left());
                handler.handle(new Either.Left<>("Error getting getSharedBookMarkUsers"));
            } else {
                JsonArray results = resultSharedBookMark.right().getValue();
                Map<String, JsonObject> uniqResults = new HashMap<>();
                if (results != null && !results.isEmpty()) {
                    for (Object objShareBook : results) {
                        JsonObject jsonShareBook = ((JsonObject) objShareBook).getJsonObject("sharedBookMark");
                        String idShareBook = jsonShareBook.getString("id");
                        idShareBook = "SB" + idShareBook;
                        jsonShareBook.put("id", idShareBook);

                        JsonObject shareBookToMerge = uniqResults.get(idShareBook);
                        if (shareBookToMerge != null) {
                            List<JsonObject> users = jsonShareBook.getJsonArray("users").getList();
                            List<JsonObject> usersToMerge = shareBookToMerge.getJsonArray("users").getList();

                            // fusion des listes sans doublon
                            users.removeAll(usersToMerge);
                            users.addAll(usersToMerge);
                            jsonShareBook.put("users", new JsonArray(users));
                        }

                        uniqResults.put(idShareBook, jsonShareBook);
                    }
                }
                handler.handle(new Either.Right<>(uniqResults));
            }
        });
    }

    private void updateAnDeleteCohorts(Handler<Either<String, JsonObject>> handler) throws UnsupportedEncodingException {
        if (arrCohortsToUpdate.isEmpty() && arrCohortsToDelete.isEmpty()) {
            String message = "Aucune cohorte à update/delete";
            log.info(message);
            endSyncGroups(handler);
        } else {
            Promise updateCohortsFuture = Promise.promise();
            Promise deleteCohortsFuture = Promise.promise();
            List<Future> listFuture = new ArrayList<>();

            if(!arrCohortsToUpdate.isEmpty()) {
                listFuture.add(updateCohortsFuture.future());
            }

            if(!arrCohortsToDelete.isEmpty()) {
                listFuture.add(deleteCohortsFuture.future());
            }


            CompositeFuture.all(listFuture).onComplete(handlerUpdateAndDeleteCohorts);

            if(!arrCohortsToUpdate.isEmpty()) {
                log.info(arrCohortsToUpdate.toString());
                updateCohorts(arrCohortsToUpdate, resultUpdate -> {
                    if (resultUpdate.isLeft()) {
                        httpClient.close();
                        handler.handle(new Either.Left<>("Error updating cohorts"));
                        log.error("Error updating cohorts", resultUpdate.left());
                        updateCohortsFuture.fail("Error updating cohorts");
                    } else {
                        log.info("End updating cohorts");
                        updateCohortsFuture.complete();
                    }
                });
            }

            if(!arrCohortsToDelete.isEmpty()) {
                deleteCohorts(arrCohortsToDelete, resultDelete -> {
                    if (resultDelete.isLeft()) {
                        httpClient.close();
                        handler.handle(new Either.Left<>("Error deleting cohorts"));
                        log.error("Error deleting cohorts", resultDelete.left());
                        deleteCohortsFuture.fail("Error deleting cohorts");
                    } else {
                        log.info("End deleting cohorts");
                        deleteCohortsFuture.complete();
                    }
                });
            }
        }
    }

    private void getUsersCoursesAndEnroll(JsonArray usersIdsToEnrrollIndivually, Handler<Either<String, JsonObject>> handler)
            throws UnsupportedEncodingException {

        if(usersIdsToEnrrollIndivually.isEmpty()) {
            String message = "No user to enrroll";
            log.info(message);
            updateAnDeleteCohorts(handler);
        } else {

            // Recupération des cours de tous ces utilisateurs
            List<Future> listGetFuture = new ArrayList<>();
            for (Object idUser: usersIdsToEnrrollIndivually) {
                listGetFuture.add(getCourses(new JsonObject().put("id", idUser)));
            }
            CompositeFuture.all(listGetFuture).onComplete(eventFuture -> {

                if (eventFuture.succeeded()) {
                    log.info("END getting courses");

                    for (Map.Entry<String, JsonObject> entryUser : mapUsersMoodle.entrySet()) {
                        JsonObject jsonUser = entryUser.getValue();
                        JsonArray jsonArrayCourses = mapUsersMoodle.get(jsonUser.getString("id")).getJsonArray("courses");

                        // Inscription individuelle a ses cours
                        identifyUserToDeleteAndEnroll(arrUsersToDelete, arrUsersToEnroll, jsonArrayCourses,
                                jsonUser.getString("id"), SyncCase.SYNC_USER_FOUND);
                    }


                    // Inscription individuel des utilisateurs à leurs cours
                    if (arrUsersToEnroll.isEmpty()) {
                        String message = "Aucune inscription individuelle necessaire (pas de cours)";
                        log.info(message);
                        try {
                            updateAnDeleteCohorts(handler);
                        } catch (UnsupportedEncodingException e) {
                            httpClient.close();
                            handler.handle(new Either.Left<>(e.getMessage()));
                            log.error("Error updateAnDeleteCohorts - UnsupportedEncodingException", e);
                        }
                        endSyncGroups(handler);
                    } else {
                        try {
                            enrollUsersIndivudually(arrUsersToEnroll, handlerEnrollGroups);
                        } catch (UnsupportedEncodingException e) {
                            httpClient.close();
                            handler.handle(new Either.Left<>(e.getMessage()));
                            log.error("Error enrollUsersIndivudually - UnsupportedEncodingException", e);
                        }
                    }
                } else {
                    httpClient.close();
                    handler.handle(new Either.Left<>("Error getting all courses in sync groups"));
                    log.error("Error getting all courses in sync groups", eventFuture.cause());
                }
            });
        }
    }
}
