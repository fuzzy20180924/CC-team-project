package io.vertx.vertx;
import io.vertx.ext.sql.ResultSet;
import io.vertx.core.AsyncResult;
import io.vertx.core.json.JsonObject;
import javafx.util.Pair;
import java.sql.*;
import java.util.*;


public class Q2MySQL {
  /**
   *  TODO: This version conforms to Guoxi's logic and creates a stateless class with
   *  TODO: one pure purpose: given a SQL query ResultSet, calculate and generate the
   *  TODO: response body (a String) suitable for returning to client.
   *  TODO: All settings related to MySQL DB connection has been delegated to the
   *  TODO: frontend code
   */
  /**
   * Class Constructor
   */
  public Q2MySQL(){
    // clean shared variable
    userRank.clear();
    rankedUserList.clear();
  }

  /**
   *  MySQL table names
   */
  private static final String tweettable = "tweet";
  private static final String usertable = "user";

  /**
     * MySQL table column names
     */
    private static final String uID = "user_id";
    private static final String tID = "target";
    private static final String text = "text";
    private static final String timestamp = "timestamp";
    private static final String weight = "weight";
    private static final String ID = "id";
    private static final String screenName = "screen_name";
    private static final String userDesc = "user_description";

    /**
     * Comparator interface
     */
    private static Comparator<Pair<Long, Double>> compareScore = new Comparator<Pair<Long, Double>>() {
        @Override
        public int compare(Pair<Long, Double> o1, Pair<Long, Double> o2) {
            return o2.getValue().compareTo(o1.getValue());
        }
    };
    private static Comparator<Pair<Long, Double>> compareID = new Comparator<Pair<Long, Double>>() {
        @Override
        public int compare(Pair<Long, Double> o1, Pair<Long, Double> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    /**
     * Cache for user information
     */
    public static HashMap<Long, HashMap<Object, Object>> userRank = new HashMap<Long, HashMap<Object, Object>>();
    public static List<Pair<Long, Double>> rankedUserList = new ArrayList<Pair<Long, Double>>();

  /**
   * doTweet(ResultSet, Long, String, Integer) - public method for calculating and ranking users by
   * their rank score
   * @param rs             ResultSet as a result of MySQL query, done in web server handler
   * @param queryID        Long number for user ID
   * @param queryPhrase   String for candidate phrase
   * @param n              Integer for no. of results to return
   * @return Upon success, return a string of comma separated user ids to be used in 2nd query in server side
   *          If exception happens, return the error message.
   * @throws Exception
   */
    public static String doTweet(List<JsonObject> rows, Long queryID, String queryPhrase, Integer n) throws Exception {
        String response = new String();
        try {
            response = getResponse(rows, queryID, queryPhrase, n);
        } catch (Exception e) {
            // in case of error, return the error message
            return e.getMessage();
        }
        return response;
    }

  /**
   * doUser(ResultSet rs) - main method for handling second part of query:
   * combine the user info and their tweet info to generate the final output
   * @param rs
   * @return A string suitable for sent back directly to client
   * TODO: may need to remove some newline characters in the generated string
   * TODO: depending upon the requested format for output
   * @throws Exception
   */
  public static String doUser(List<JsonObject> rows) throws Exception {
      String response = "";
      try {
        response = generateOutput(rows);
      } catch (Exception e) {
        return e.getMessage();
      }
      return response;
    }

  /**
   * getResponse(ResultSet, Long, String, int) - helper function for doTweet()
   * @param rs
   * @param queryID
   * @param phrase
   * @param n
   * @return  A string of comma-separated user ids for generating query for the 2nd part
   * @throws Exception
   */
    private static String getResponse(List<JsonObject> rows, Long queryID, String phrase, int n) throws Exception {
        // calculate rank scores and rank users
        rankedUserList = calculateResult(rows, queryID, phrase, n);
        // generate the query string for list of top users
        String userString = "";
        for (Pair<Long, Double> pair: rankedUserList) {
            userString += pair.getKey().toString()+", ";
        }
        userString = userString.substring(0, userString.length()-2); // remove the last ","
        return userString;
    }

    /**
     * calculate intimacy and phrase scores, and rank users, helper function for doTweet()
     * @param rs
     * @param phrase
     * @param n
     * @return
     */
    private static List<Pair<Long, Double>> calculateResult(List<JsonObject> rows, Long queryID, String phrase, int n) {
        String intimacyCnt = "intimacy_count";
        String phraseCnt = "phrase_count";
        String newTweet = "latest_tweet";
        String currCount = "current_count";  // used to compare and save only the tweets with largest subphrase count
        String currTimestamp = "current_timestamp";  // used to compare and save only the latest tweet, if count is the same
        String rankScore = "rank_score";
        Long id = 0L;
        // clear previous cache
        userRank.clear();
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
                if (userRank.containsKey(id)) {
                    // encountered this guy before
                    // update intimacy count
                    int oldvalue = (Integer) userRank.get(id).get(intimacyCnt);
                    userRank.get(id).put(intimacyCnt, oldvalue + rs.getInteger(weight));
                    // update phrase count
                    oldvalue = (Integer) userRank.get(id).get(phraseCnt);
                    String tweettext = rs.getString(text);
                    int count = tweettext.toLowerCase().split(phrase.toLowerCase()).length - 1;
                    userRank.get(id).put(phraseCnt, oldvalue + count);
                    // update the saved tweet, if necessary
                    int oldcount = (Integer) userRank.get(id).get(currCount);
                    if (count > oldcount) {
                        // a new tweet with larger phrase count found
                        userRank.get(id).put(newTweet, rs.getString(text));
                        userRank.get(id).put(currCount, count);
                    } else if (count == oldcount) {
                        // a new tweet with the same count but newer timestamp found
                        Long newTS = rs.getLong(timestamp);
                        Long oldTS = (Long) userRank.get(id).get(currTimestamp);
                        if (oldTS < newTS) {
                            userRank.get(id).put(newTweet, tweettext);
                            userRank.get(id).put(currTimestamp, newTS);
                        }
                    }
                } else {
                    userRank.put(id, new HashMap<Object, Object>());
                    userRank.get(id).put(intimacyCnt, rs.getInteger(weight));
                    String tweettext = rs.getString(text);
                    int count = tweettext.toLowerCase().split(phrase.toLowerCase()).length - 1;
                    userRank.get(id).put(phraseCnt, count);
                    userRank.get(id).put(currCount, count);
                    userRank.get(id).put(currTimestamp, rs.getLong(timestamp));
                    userRank.get(id).put(newTweet, rs.getString(text));
                }
            }
            // calculate rank score
            final LinkedList<Pair<Long, Double>> users = new LinkedList<Pair<Long, Double>>();
            for (Long user: userRank.keySet()) {
                double score = (1 + Math.log(1 + (Integer)userRank.get(user).get(intimacyCnt)));
                score *= (Integer)userRank.get(user).get(phraseCnt) + 1;
                users.add(new Pair(user, score));
            }
            // sort the list of users by score (descending) and user_id (ascending)
            Collections.sort(users, compareScore.thenComparing(compareID));
            // return results
            if (users.size() <= n) {
                return users;
            } else {
                List<Pair<Long, Double>> topNusers = users.subList(0, n);
                return topNusers;
            }
    }

  /**
   * generateOutput(ResultSet rs) - use the result from 2nd query into "user" table
   * AND shared class variables, "rankedUserList" and "userRank", to generate final
   * string suitable for returning as response to client. Helper function for doUser()
   * @param rs
   * @return
   */
    private static String generateOutput(List<JsonObject> rows) {
      String output = "";
      for (JsonObject rs : rows) {
        Long user = rs.getLong(ID);
        String sName = rs.getString(screenName);
        String desc = rs.getString(userDesc);
        userRank.get(user).put(screenName, sName);
        userRank.get(user).put(userDesc, desc);
      }
      // now out put list of strings in order
      for (Pair<Long, Double> userscorepair : rankedUserList) {
        Long user = userscorepair.getKey();
        String outputLn = (String) userRank.get(user).getOrDefault(screenName, "");
        outputLn += "\t";
        outputLn += userRank.get(user).getOrDefault(userDesc, "");
        outputLn += "\t";
        outputLn += userRank.get(user).get("latest_tweet");
        outputLn += "\n";
        output += outputLn;
      }
      return output;
    }

}
