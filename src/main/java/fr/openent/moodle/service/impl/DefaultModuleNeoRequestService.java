package fr.openent.moodle.service.impl;

import fr.openent.moodle.service.ModuleNeoRequestService;
import fr.wseduc.webutils.Either;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.entcore.common.neo4j.Neo4j;
import org.entcore.common.neo4j.Neo4jResult;

public class DefaultModuleNeoRequestService implements ModuleNeoRequestService {

    @Override
    public void getUsers(final JsonArray usersIds, Handler<Either<String, JsonArray>> handler) {
        JsonObject params = new JsonObject()
                .put("usersIds", usersIds);

        String queryUsersNeo4j =
                "MATCH (u:User) WHERE  u.id IN $usersIds " +
                        "RETURN u.id AS id, u.id as username, u.email AS email, u.firstName AS firstname, u.lastName AS lastname";

        Neo4j.getInstance().execute(queryUsersNeo4j, params, Neo4jResult.validResultHandler(handler));
    }

    @Override
    public void getGroups(final JsonArray groupsIds, Handler<Either<String, JsonArray>> handler) {
        JsonObject params = new JsonObject()
                .put("groupsIds", groupsIds);


        String queryGroupsNeo4j =
                "MATCH(g:Group)-[:IN]-(ug:User) WHERE g.id  IN $groupsIds " +
                        "WITH g, collect({id: ug.id, username: ug.id, email: ug.email, firstname: ug.firstName, lastname: ug.lastName}) " +
                        "AS users " +
                        "return \"GR_\"+g.id AS id, g.name AS name, users";

        Neo4j.getInstance().execute(queryGroupsNeo4j, params, Neo4jResult.validResultHandler(handler));
    }

    @Override
    public void getSharedBookMark(final JsonArray bookmarksIds, Handler<Either<String, JsonArray>> handler) {
        JsonObject params = new JsonObject()
                .put("bookmarksIds", bookmarksIds);

        String queryNeo4j = "WITH $bookmarksIds AS shareBookmarkIds " +
                "UNWIND shareBookmarkIds AS shareBookmarkId MATCH (u:User)-[:HAS_SB]->(sb:ShareBookmark) " +
                "UNWIND TAIL(sb[shareBookmarkId]) as vid MATCH (v:Visible {id : vid}) WHERE v.deleteDate IS NULL " +
                "WITH {group: {id: \"SB\" + shareBookmarkId, name: HEAD(sb[shareBookmarkId]), users: COLLECT(DISTINCT{id: v.id, " +
                "email: v.email, lastname: v.lastName, firstname: v.firstName, username: v.id})}}as sharedBookMark " +
                "RETURN COLLECT(sharedBookMark) as bookmarks;";

        Neo4j.getInstance().execute(queryNeo4j, params, Neo4jResult.validResultHandler(handler));
    }

    @Override
    public void getSharedBookMarkUsers(final JsonArray bookmarksIds, Handler<Either<String, JsonArray>> handler) {
        JsonObject params = new JsonObject()
                .put("bookmarksIds", bookmarksIds);

        String queryNeo4j = "WITH $bookmarksIds AS shareBookmarkIds UNWIND shareBookmarkIds AS shareBookmarkId " +
                "MATCH (u:User)-[:HAS_SB]->(sb:ShareBookmark) UNWIND TAIL(sb[shareBookmarkId]) as vid " +
                "MATCH (v:Visible {id : vid})<-[:IN]-(us:User) WHERE v.deleteDate IS NULL and v:ProfileGroup " +
                "WITH {id: shareBookmarkId, name: HEAD(sb[shareBookmarkId]), " +
                "users: COLLECT(DISTINCT{id: us.id, email: us.email, lastname: us.lastName, firstname: us.firstName, username: us.id})} " +
                "as sharedBookMark " +
                "RETURN sharedBookMark " +
                "UNION " +
                "WITH $bookmarksIds AS shareBookmarkIds UNWIND shareBookmarkIds AS shareBookmarkId " +
                "MATCH (u:User)-[:HAS_SB]->(sb:ShareBookmark) UNWIND TAIL(sb[shareBookmarkId]) as vid " +
                "MATCH (v:Visible {id : vid}) WHERE v.deleteDate IS NULL and v:User " +
                "WITH {id: shareBookmarkId, name: HEAD(sb[shareBookmarkId]), " +
                "users: COLLECT(DISTINCT{id: v.id, email: v.email, lastname: v.lastName, firstname: v.firstName, username: v.id})} " +
                "as sharedBookMark " +
                "RETURN sharedBookMark";

        Neo4j.getInstance().execute(queryNeo4j, params, Neo4jResult.validResultHandler(handler));
    }
}
