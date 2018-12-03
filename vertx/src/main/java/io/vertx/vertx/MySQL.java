
package io.vertx.vertx;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Objects;

import javax.xml.ws.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javafx.util.Pair;

import org.apache.commons.dbcp2.BasicDataSource;

public class MySQL {
    
    // private static final String IN = " IN (?) ";
    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "db";

    /**
     * e.g., before running "mvn clean package exec:java" to start the server
     * run the following commands to set the environment variables.
     * export MYSQL_HOST=...
     * export MYSQL_NAME=...
     * export MYSQL_PWD=...
     */
    private static String MYSQL_HOST = System.getenv("MYSQL_HOST");
    private static String MYSQL_NAME = System.getenv("MYSQL_NAME");
    private static String MYSQL_PWD = System.getenv("MYSQL_PWD");
    private static Connection conn;
    private static final String URL = "jdbc:mysql://" + MYSQL_HOST + ":3306/"
            + DB_NAME + "?useSSL=false";
    private static BasicDataSource ds = new BasicDataSource();
    /**
     * MySQL table names
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
    private static final String screenName = "user_screen_name";
    private static final String userDesc = "user_description";
    /**
     * MySQL query skeleton
     */
    private static String tweetQuery = "SELECT *"
            + " FROM " + tweettable
            + " WHERE " + uID + " = ? "
            + " OR " + tID + " = ? ";
    private static String userQuery = "SELECT * "
            + " FROM " + usertable
            + " WHERE " + ID + " IN (?)";
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
    private static HashMap<Long, HashMap<Object, Object>> userRank = new HashMap<Long, HashMap<Object, Object>>();


    /**
     * Initialize SQL connection.
     *
     * @throws ClassNotFoundException when an application fails to load a class
     * @throws SQLException           on a database access error or other errors
     */
    public MySQL() throws ClassNotFoundException, SQLException {
        System.out.println(MYSQL_HOST);
        System.out.println(MYSQL_NAME);
        System.out.println(MYSQL_PWD);
        Class.forName(JDBC_DRIVER);
        Objects.requireNonNull(MYSQL_HOST);
        Objects.requireNonNull(MYSQL_NAME);
        Objects.requireNonNull(MYSQL_PWD);
        ds.setDriverClassName("com.mysql.jdbc.Driver");
        ds.setUrl(URL);
        ds.setUsername(MYSQL_NAME);
        ds.setPassword(MYSQL_PWD);
        ds.setInitialSize(10);
        ds.setMaxTotal(50);
        ds.setMaxIdle(20);
        ds.setMaxWaitMillis(60000);
        ds.setMinIdle(5);
        // conn = DriverManager.getConnection(URL, MYSQL_NAME, MYSQL_PWD);
    }

    // public String Query() throws SQLException {
        // String test_q = "SELECT * FROM user LIMIT 1;";
        // Statement stmt = conn.createStatement();
        // ResultSet rs = stmt.executeQuery(test_q);
        // while (rs.next()) {
        //     String result = rs.getString("User");
        //     return result;
        // }
        // return "null";
    // }
    /**
     * Main entry.
     *
     * Should be called with all 3 query parameters in the following order:
     * 1. query user id
     * 2. query phrase (for Phrasescore calculation)
     * 3. query no of user (to be parsed as integer)
     *
     * @param args The arguments for main method.
     */
    public static String Query(String queryID, String phrase, String n) throws Exception {
        conn = (Connection) ds.getConnection();
        if (queryID.equals("null") || phrase.equals("null") || n.equals("null")) {
            // return getResponse(1703784498L, "Open", 5);
            // TODO: comment this out during actual run
            // TODO: uncomment the following during actual run
            showUsage();
            System.exit(1);
        }
        String response = new String();
        try {
            response = getResponse(Long.parseLong(queryID), phrase, Integer.parseInt(n));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            cleanup();
        }
        return response;
    }

    /**
     * Clean up and terminate the connection.
     */
    private static void cleanup() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Main program for extracting response from MySQL
     */
    private static String getResponse(Long queryID, String phrase, int n) throws SQLException {
        queryID = 1608836484L;
        phrase = "Counter%20Spy";
        n = 10;
        // build prepared statement
        PreparedStatement pstmt = conn.prepareStatement(tweetQuery);
        // set the statement parameters
        pstmt.setLong(1, queryID);
        pstmt.setLong(2, queryID);
        // execute statement
        long startTime = System.currentTimeMillis();
        ResultSet rs = pstmt.executeQuery();
        // calculate rank scores and rank users
        List<Pair<Long, Double>> rankedUser = calculateResult(rs, queryID, phrase, n);
        // close the query
        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        // generate the query string for list of top users
        String userString = "";
        for (Pair<Long, Double> pair: rankedUser) {
            userString += pair.getKey().toString()+", ";
        }
        userString = userString.substring(0, userString.length()-2); // remove the last ","
        // query MySQL for user information
        // return the required output
        PreparedStatement ustmt = conn.prepareStatement(userQuery);
        ustmt.setString(1, userString);
        rs = ustmt.executeQuery();
        String output = generateOutput(rs, rankedUser);
        // close the query
        if (ustmt != null) {
            try {
                ustmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return output;
    }

    /*
     * The methods below are the helper methods.
     *
     */

    /**
     * calculate intimacy and phrase scores, and rank users
     * @param rs
     * @param phrase
     * @param n
     * @return
     */
    private static List<Pair<Long, Double>> calculateResult(ResultSet rs, Long queryID, String phrase, int n) {
        String intimacyCnt = "intimacy_count";
        String phraseCnt = "phrase_count";
        String newTweet = "latest_tweet";
        String currCount = "current_count";  // used to compare and save only the tweets with largest subphrase count
        String currTimestamp = "current_timestamp";  // used to compare and save only the latest tweet, if count is the same
        String rankScore = "rank_score";
        Long id = 0L;
        // clear previous cache
        userRank.clear();
        try {
            while (rs.next()) {
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
                    userRank.get(id).put(intimacyCnt, oldvalue + rs.getInt(weight));
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
                    userRank.get(id).put(intimacyCnt, rs.getInt(weight));
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * generate output from the collected users and tweets
     */
    private static String generateOutput(ResultSet rs, List<Pair<Long, Double>> userList) {
        LinkedList<String> results = new LinkedList<String>();
        String output = "";
        try {
            while (rs.next()) {
                Long user = rs.getLong(ID);
                String sName = rs.getString(screenName);
                String desc = rs.getString(userDesc);
                userRank.get(user).put(screenName, sName);
                userRank.get(user).put(userDesc, desc);
            }
            // now out put list of strings in order
            for (Pair<Long, Double> userscorepair: userList) {
                Long user = userscorepair.getKey();
                String outputLn = (String) userRank.get(user).getOrDefault(screenName, "");
                outputLn += "\t";
                outputLn += userRank.get(user).getOrDefault(userDesc, "");
                outputLn += "\t";
                outputLn += userRank.get(user).get("latest_tweet");
                outputLn += "\n";
                output += outputLn;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            return output;
        }
    }

    /**
     * Show the usage guide for this program.
     */
    private static void showUsage() {
        String jarPath = "not defined";
        String className = "Q2MySQL";
        System.out.println(
                String.format("Usage: java -cp %s %s <question>",
                        jarPath, className));
        System.out.println(
                String.format("Usage: java -cp %s %s demo",
                        jarPath, className));
    }

    /**
     * Get the custom index(es) given the table name.
     *
     * The primary key and foreign key (on business_id) created by
     * create_yelp_database.sql will be excluded.
     *
     * @return indexes with K: INDEX_NAME, V: COLUMN_NAME
     */
    private static Map<String, String> getIndexes(String tableName) {
        String query =
                String.format("SELECT INDEX_NAME, COLUMN_NAME FROM "
                        + "INFORMATION_SCHEMA.STATISTICS "
                        + "WHERE table_schema = '%s' AND table_name = "
                        + "'%s'", DB_NAME, tableName);
        Statement stmt = null;
        Map<String, String> indexes = new HashMap();
        try {
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                if (!indexName.equals("PRIMARY")
                        && !indexName.equals("user_id")
                        && !indexName.equals("business_id")) {
                    indexes.put(indexName, columnName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return indexes;
    }
}