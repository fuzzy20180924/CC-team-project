package io.vertx.vertx;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.core.AbstractVerticle;
import io.vertx.ext.web.RoutingContext;
import io.vertx.core.Future;

import java.util.*;


public class MainVerticle extends AbstractVerticle {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "db";
    private static String MYSQL_HOST = System.getenv("MYSQL_HOST");
    private static String MYSQL_NAME = System.getenv("MYSQL_NAME");
    private static String MYSQL_PWD = System.getenv("MYSQL_PWD");
    private static final String URL = "jdbc:mysql://" + MYSQL_HOST + ":3306/"
            + DB_NAME + "?useSSL=false";
    private static String TEAMID = "fuzzy20180924";
    private static String TEAM_AWS_ACCOUNT_ID = "863363867791";
    private static JDBCClient SQLClient = null;
    private static String tweetQuery = "SELECT *"
      + " FROM " +"tweet"
      + " WHERE " + "user_id" + " = ? "
      + " OR " + "target" + " = ? ";
    private static String userQuery = "SELECT * "
      + " FROM " + "user"
      + " WHERE " + "id" + " in (";
    private static String q3Query = "SELECT *" + " FROM q3notf"
    + " WHERE user_id BETWEEN ? AND ? AND timestamp BETWEEN ? AND ?;";

  private static String calculateUserRanks(List<JsonObject> rows, Long queryID, String phrase, Integer n, JsonArray buffer) {
    String uID = "user_id";
    String tID = "target";
    String text = "text";
    String timestamp = "timestamp";
    String weight = "weight";
    String intimacyCnt = "intimacy_count";
    String phraseCnt = "phrase_count";
    String newTweet = "latest_tweet";
    String currCount = "current_count";  // used to compare and save only the tweets with largest subphrase count
    String currTimestamp = "current_timestamp";  // used to compare and save only the latest tweet, if count is the same
    Long id = 0L;
    // clear previous cache
    JsonObject userRank = new JsonObject();
    //Map<Long, HashMap<String, String>> userRank = new HashMap<Long, HashMap<String, String>>();
    for (JsonObject  rs:rows) {
      // get the contact user id
      Long uid = rs.getLong(uID);
      Long tid = rs.getLong(tID);
      if (uid.equals(queryID)) {
        id = tid;
      } else {
        id = uid;
      }
      // update scores for the contact user
      if (userRank.containsKey(id.toString())) {
        // encountered this guy before
        // update intimacy count
        Integer oldvalue = Integer.parseInt(userRank.getJsonObject(id.toString()).getString(intimacyCnt));
        oldvalue += rs.getInteger(weight);
        userRank.getJsonObject(id.toString()).put(intimacyCnt, oldvalue.toString());
        // update phrase count
        oldvalue = Integer.parseInt(userRank.getJsonObject(id.toString()).getString(phraseCnt));
        String tweettext = rs.getString(text);
        Integer count = countPhrase(tweettext, phrase);;
        oldvalue += count;
        userRank.getJsonObject(id.toString()).put(phraseCnt, oldvalue.toString());
        // update the saved tweet, if necessary
        Integer oldcount = Integer.parseInt(userRank.getJsonObject(id.toString()).getString(currCount));
        if (count > oldcount) {
          // a new tweet with larger phrase count found
          userRank.getJsonObject(id.toString()).put(newTweet, rs.getString(text));
          userRank.getJsonObject(id.toString()).put(currCount, count.toString());
        } else if (count == oldcount) {
          // a new tweet with the same count but newer timestamp found
          Long newTS = rs.getLong(timestamp);
          Long oldTS = Long.parseLong(userRank.getJsonObject(id.toString()).getString(currTimestamp));
          if (oldTS < newTS) {
            userRank.getJsonObject(id.toString()).put(newTweet, tweettext);
            userRank.getJsonObject(id.toString()).put(currTimestamp, newTS.toString());
          }
        }
      } else {

        JsonObject newUser = new JsonObject();
        newUser.put(intimacyCnt, rs.getInteger(weight).toString());
        String tweettext = rs.getString(text);
        Integer count = countPhrase(tweettext, phrase);
        newUser.put(phraseCnt, count.toString());
        newUser.put(currCount, count.toString());
        newUser.put(currTimestamp, rs.getLong(timestamp).toString());
        newUser.put(newTweet, rs.getString(text));
        userRank.put(id.toString(), newUser);
      }
    }
    LinkedList<JsonObject> users = new LinkedList<JsonObject>();
    Iterator<Map.Entry<String,Object>> iter = userRank.iterator();
    while (iter.hasNext()) {
      Map.Entry<String, Object> keyValue = iter.next();
      String user = keyValue.getKey();
      JsonObject content = (JsonObject) keyValue.getValue();
      double score = (1 + Math.log(1 + Integer.parseInt(content.getString(intimacyCnt))));
      score *= Integer.parseInt(content.getString(phraseCnt)) + 1;
      JsonObject user_score = new JsonObject();
      user_score.put("user_id", Long.parseLong(user));
      user_score.put("score", score);
      users.add(user_score);
    }
    // sort the list of users by score (descending) and user_id (ascending)
    Collections.sort(users, compareScore.thenComparing(compareID));
    // return results
    JsonArray topUsers = null;
    if (users.size() <= n) {
      topUsers = new JsonArray(users);

    } else {
      topUsers = new JsonArray(users.subList(0, n));
    }
    userRank.put("topUsers", topUsers);
    String userString = "";
    for (int x = 0; x < topUsers.size(); x++) {
      JsonObject u = (JsonObject)topUsers.getValue(x);
      userString += u.getLong("user_id").toString()+", ";
    }
    userString = userString.substring(0, userString.length()-2); // remove the last ","
    userRank.put("userQueryString", new JsonArray().add(userString));
    buffer.add(userRank);
    return userString;
  }

  private static Comparator<JsonObject> compareScore = new Comparator<JsonObject>() {
    @Override
    public int compare(JsonObject o1, JsonObject o2) {
      return o2.getDouble("score").compareTo(o1.getDouble("score"));
    }
  };
  private static Comparator<JsonObject> compareID = new Comparator<JsonObject>() {
    @Override
    public int compare(JsonObject o1, JsonObject o2) {
      return o1.getLong("user_id").compareTo(o2.getLong("user_id"));
    }
  };

  private static String generateResponseBody(List<JsonObject> rows, JsonArray buffer, RoutingContext routingContext) {
    String uID = "user_id";
    String tID = "target";
    String text = "text";
    String timestamp = "timestamp";
    String weight = "weight";
    String ID = "id";
    String screenName = "screen_name";
    String userDesc = "user_description";
    String output = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
    JsonObject userRank = buffer.getJsonObject(0);

    for (JsonObject rs: rows) {
      Long user = rs.getLong(ID);
      String sName = rs.getString(screenName);
      String desc = rs.getString(userDesc);
      userRank.getJsonObject(user.toString()).put(screenName, sName);
      userRank.getJsonObject(user.toString()).put(userDesc, desc);
    }
    // now out put list of strings in order
    JsonArray rankedUserList = userRank.getJsonArray("topUsers");
    Iterator<Object> iter = rankedUserList.iterator();
    while (iter.hasNext()) {
      JsonObject p= (JsonObject)iter.next();
      JsonObject u = userRank.getJsonObject(p.getLong("user_id").toString());
      String outputLn = "";
      if (u.containsKey(screenName)){
        String tmp = u.getString(screenName);
        outputLn += (tmp == null) ? "" : tmp;
      }
      outputLn += "\t";
      if (u.containsKey(userDesc)) {
        String tmp = u.getString(userDesc);
        outputLn += (tmp == null) ? "" : tmp;
      }
      outputLn += "\t";
      outputLn += u.getString("latest_tweet");
      outputLn += "\n";
      output += outputLn;
    }
    HttpServerResponse response = routingContext.response();
    response
      .putHeader("content-type", "text/html")
      .end(output.substring(0, output.length()-1));
    return output;
  }

  /**
   *  Helper function for counting number of appearance of phrases in tweets
   * @throws Exception
   */
  private static int countPhrase(String tweettext, String phrase) {
    if (tweettext == null) {
      return 0;
    }
    if (tweettext.length() < phrase.length()) {
      return 0;
    }
    int count = 0;
    int index = 0;
    int phlen = phrase.length();
    int twlen = tweettext.length();
    while ((index = tweettext.indexOf(phrase, index))!= -1) {
      if (index == 0 && tweettext.charAt(phlen) == ' ') {
        // found a match at the start
        count++;
      } else if (tweettext.charAt(index - 1) == ' ' && index + phlen == twlen) {
        // found a match at the end
        count++;
      } else if (tweettext.charAt(index - 1) == ' ' && tweettext.charAt(index+phlen) == ' '){
        // found a match within string
        count++;
      }
      index += phlen;
    }
    return count;
  }

  Future<ResultSet> futureHandler (String uid, SQLConnection conn){
      Future<ResultSet> fu = Future.future();
      conn.query(userQuery + uid + ";", res -> {
          if (res.succeeded()) {
              // List<ResultSet> rs = res.result().getResults();
              ResultSet userrs = res.result();
              fu.complete(userrs);
          } else {
              System.out.println("res failed.");
          }
      });
      return fu;
      // futureQs.add(fut); // getUserInfo is fut type
  }

//REF https://medium.com/@levon_t/java-vert-x-starter-guide-part-1-30cb050d68aa
  // Compile: sudo mvn package -DskipTestsar
  // run: java -jar target/vertx-1.0.0-SNAPSHOT-fat.jar
  // in browser access http://localhost:8080/q2?user_id=1608836484&phrase=Counter%20Spy&n=5
    public void start() throws Exception {
        System.out.println(MYSQL_HOST);
        System.out.println(MYSQL_NAME);
        System.out.println(MYSQL_PWD);
        JsonObject config = new JsonObject()
          .put("url", URL)
          .put("driver_class", JDBC_DRIVER)
          .put("max_pool_size", 100)
          .put("user", MYSQL_NAME)
          .put("password", MYSQL_PWD);
        JDBCClient jdbc = JDBCClient.createShared(vertx, config);

        Router router = Router.router(vertx);

        router.route(HttpMethod.GET, "/q3").handler(routingContext -> {
            String defaultResponse = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
            MultiMap param = routingContext.request().params();
            HttpServerResponse response = routingContext.response();
            Long uid_start_trial = null;
            Long uid_end_trial = null;
            Long time_start_trial = null;
            Long time_end_trial = null;
            Integer n1_trial = null;
            Integer n2_trial = null;
            try {
                uid_start_trial = Long.parseLong(param.get("uid_start"));
                uid_end_trial = Long.parseLong(param.get("uid_end"));
                time_start_trial = Long.parseLong(param.get("time_start"));
                time_end_trial = Long.parseLong(param.get("time_end"));
                n1_trial = Integer.parseInt(param.get("n1"));
                n2_trial = Integer.parseInt(param.get("n2"));
            } catch (Exception e) {
                response.end(defaultResponse);
            }
            if (uid_start_trial <= 0 || uid_end_trial <= 0 || time_start_trial <= 0
              || time_end_trial <= 0 || n1_trial <= 0 || n2_trial <= 0) {
                response.end(defaultResponse);
            }
            final Long uid_start = uid_start_trial;
            final Long uid_end = uid_end_trial;
            final Long time_start = time_start_trial;
            final Long time_end = time_end_trial;
            final Integer n1 = n1_trial;
            final Integer n2 = n2_trial;
            JsonArray queryParam = new JsonArray().add(uid_start).add(uid_end).add(time_start).add(time_end);
            jdbc.getConnection(aconn->{
                if (aconn.succeeded()) {
                      Future fi = Future.future();
                      JsonArray buffer = new JsonArray();
                      SQLConnection conn = aconn.result();
                      Future<ResultSet> query1 = Future.future();
		      System.out.println("query sent");
                      conn.queryWithParams(q3Query, queryParam, query1.completer());
		      System.out.println("query done");
                      query1.compose(v ->{
                          String output = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
                          Q3MySQLnew q3 = new Q3MySQLnew();
			  System.out.println("before generating response.");
                          String r = q3.getResponse(v.getRows(), n1,n2);
			  System.out.println("response generated.");
                          output += r;
                          response.putHeader("content-type", "text/html")
                          .end(output);
                          conn.close();
                          fi.complete();},fi);
                } else {
                  routingContext.fail(aconn.cause());
                }
            });
        });

        router.route(HttpMethod.GET, "/q2").handler(routingContext -> {
            String defaultResponse = TEAMID + "," + TEAM_AWS_ACCOUNT_ID + "\n";
            MultiMap param = routingContext.request().params();
            HttpServerResponse response = routingContext.response();
            // filter malformed request first
            Long user_id_trial = null;
            final String phrase = param.get("phrase");
            Integer n_trial = 0;
            try {
              user_id_trial = Long.parseLong(param.get("user_id"));
              n_trial = Integer.parseInt(param.get("n"));
            } catch (Exception e) {
              response.end(defaultResponse);
            }
            // filter non-sensical values
            if (user_id_trial <= 0 || n_trial <= 0 || phrase.length() == 0) {
              response.end(defaultResponse);
            }
            // finalize variables
            final Long user_id = user_id_trial;
            final Integer n = n_trial;

            JsonArray queryParam = new JsonArray().add(user_id).add(user_id);
            jdbc.getConnection(aconn->{
              if (aconn.succeeded()) {
                Future fi = Future.future();
                JsonArray buffer = new JsonArray();
                  SQLConnection conn = aconn.result();
                  Future<ResultSet> query1 = Future.future();
                  conn.queryWithParams(tweetQuery, queryParam, query1.completer());
                  query1.compose(v ->{
                      String qString = calculateUserRanks(v.getRows(), user_id, phrase, n, buffer);
                      Future<ResultSet> query2 = Future.future();
                      conn.query(userQuery+qString+");", query2.completer());
                      return query2;
                }).compose(v->{
                  generateResponseBody(v.getRows(), buffer,routingContext);
                  conn.close();
                  fi.complete();
                },fi);
              } else {
                routingContext.fail(aconn.cause());
              }
            });
          });

        router.route(HttpMethod.GET, "/q1").handler(routingContext -> {
            String type = routingContext.request().getParam("type");
            String data = routingContext.request().getParam("data");
            HttpServerResponse response = routingContext.response();
            String res = "Invalid";
            if (type.equals("encode")) {
                if (data.length() > 22 || data.length() == 0) {
                    res = "\nCannot deal with the request.\n";
                } else {
                    res = "encode";
                    // QRencoder QRencoder = new QRencoder();
                    try {
                        res = QRencoder.encode(data);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            } else if(type.equals("decode")) {
                if (data.length() <= 0) {
                    res = "\nCannot deal with the request.\n";
                } else {
                    res = "decode";
                    // res = data;
                    // _, res = QRdecode(data); // TODO: remove 1st return
                    try {
                        res = QRdecoder.decode(data);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            } else {
                res = "\nCannot detect request type.\n";
            }
            response
                .putHeader("content-type", "text/html")
                .end(res);
            });
            // Create the HTTP server and pass the "accept" method to the request handler.
    vertx.createHttpServer() // <4>
      .requestHandler(router::accept)
      .listen(80, result -> {

      });

  }
}


