package io.vertx.vertx;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.*;

// TODO: This version sometimes generate topic score a bit higher than actual value, still needs debug
// TODO: but may be correct
public class Q3MySQL {

  /**
   *  The constructor.
   */
  public Q3MySQL() {

  }
  /**
   * MySQL table column names
   */
  private static final String uID = "user_id";
  private static final String TF = "TF";
  private static final String text = "text";
  private static final String textCensor = "textCensored";
  private static final String timestamp = "timestamp";
  private static final String ID = "id";
  private static final String IS = "impactScore";
  /**
   * MySQL resultSet collection lists
   */

  /**
   * Following is the hard-coded censor words set
   */
  private static final Set<String> censored = new HashSet<String>(Arrays.asList("15619cctest", "4r5e", "5h1t", "5hit", "n1gga", "n1gger", "nobhead", "nobjocky", "nobjokey", "nutsack", "numbnuts", "nazi", "nigg3r", "nigg4h", "nigga", "niggas", "niggaz", "niggah", "nigger", "niggers", "omg", "p0rn", "poop", "pron", "prick", "pricks", "pussy", "pussys", "pusse", "pussi", "pussies", "pube", "pawn", "penis", "penisfucker", "phonesex", "phuq", "phuck", "phuk", "phuks", "phuked", "phuking", "phukked", "phukking", "piss", "pissoff", "pisser", "pissers", "pisses", "pissflaps", "pissin", "pissing", "pigfucker", "pimpis", "queer", "rectum", "rimjaw", "rimming", "snatch", "sonofabitch", "spunk", "scrotum", "scrote", "scroat", "schlong", "sex", "semen", "sh1t", "shit", "shits", "shitty", "shitter", "shitters", "shitted", "shitting", "shittings", "shitdick", "shite", "shitey", "shited", "shitfuck", "shitfull", "shithead", "shiting", "shitings", "skank", "slut", "smut", "smegma", "tosser", "turd", "tw4t", "twunt", "twunter", "twat", "twatty", "twathead", "teets", "tit", "tittywank", "tittyfuck", "titties", "tittiefucker", "titwank", "titfuck", "v14gra", "v1gra", "vulva", "vagina", "viagra", "w00se", "wtff", "wang", "wank", "wanky", "wanker", "whore", "whore4r5e", "whoreshit", "whoreanal", "whoar", "a55", "anus", "anal", "arse", "ass", "assram", "asswhole", "assfucker", "assfukka", "assho", "b00bs", "b17ch", "b1tch", "boner", "booooooobs", "booooobs", "boooobs", "booobs", "boob", "boobs", "boiolas", "bollock", "bollok", "breasts", "bunnyfucker", "butt", "buttplug", "buttmuch", "buceta", "bugger", "bullshit", "bum", "bastard", "balls", "ballsack", "bestial", "bestiality", "beastial", "beastiality", "bellend", "bitch", "biatch", "bloody", "blowjob", "blowjobs", "c0ck", "c0cksucker", "cnut", "coon", "cox", "cock", "cocks", "cocksuck", "cocksucks", "cocksucker", "cocksucked", "cocksucking", "cocksuka", "cocksukka", "cockface", "cockhead", "cockmunch", "cockmuncher", "cok", "coksucka", "cokmuncher", "crap", "cunnilingus", "cunt", "cunts", "cuntlick", "cuntlicker", "cuntlicking", "cunilingus", "cunillingus", "cum", "cums", "cumshot", "cummer", "cumming", "cyalis", "cyberfuc", "cyberfuck", "cyberfucker", "cyberfuckers", "cyberfucked", "cyberfucking", "carpetmuncher", "cawk", "chink", "cipa", "cl1t", "clit", "clitoris", "clits", "d1ck", "donkeyribber", "doosh", "dogfucker", "doggin", "dogging", "duche", "dyke", "damn", "dink", "dinks", "dirsa", "dick", "dickhead", "dildo", "dildos", "dlck", "ejaculate", "ejaculates", "ejaculated", "ejaculating", "ejaculatings", "ejaculation", "ejakulate", "f4nny", "fook", "fooker", "fux", "fux0r", "fuck", "fucks", "fuckwhit", "fuckwit", "fucka", "fucker", "fuckers", "fucked", "fuckhead", "fuckheads", "fuckin", "fucking", "fuckings", "fuckingshitmotherfucker", "fuckkkdatttbitchhhh", "fuckme", "fudgepacker", "fuk", "fuks", "fukwhit", "fukwit", "fuker", "fukker", "fukkin", "fannyfucker", "fannyflaps", "fanyy", "fag", "fagot", "fagots", "fags", "faggot", "faggs", "fagging", "faggitt", "fcuk", "fcuker", "fcuking", "feck", "fecker", "felching", "fellate", "fellatio", "fingerfuck", "fingerfucks", "fingerfucker", "fingerfuckers", "fingerfucked", "fingerfucking", "fistfuck", "fistfucks", "fistfucker", "fistfuckers", "fistfucked", "fistfucking", "fistfuckings", "flange", "goatse", "goddamn", "gangbang", "gangbangs", "gangbanged", "gaysex", "horny", "horniest", "hore", "hotsex", "hoar", "hoare", "hoer", "homo", "hardcoresex", "heshe", "hell", "jap", "jackoff", "jerk", "jerkoff", "jism", "jiz", "jizz", "jizm", "knob", "knobend", "knobead", "knobed", "knobhead", "knobjocky", "knobjokey", "kondum", "kondums", "kock", "kunilingus", "kum", "kums", "kummer", "kumming", "kawk", "l3itch", "l3ich", "lust", "lusting", "labia", "lmao", "lmfao", "m0f0", "m0fo", "m45terbate", "mothafuck", "mothafucks", "mothafucka", "mothafuckas", "mothafuckaz", "mothafucker", "mothafuckers", "mothafucked", "mothafuckin", "mothafucking", "mothafuckings", "motherfuck", "motherfucks", "motherfucker", "motherfuckers", "motherfucked", "motherfuckin", "motherfucking", "motherfuckings", "motherfuckka", "mof0", "mofo", "mutha", "muthafuckker", "muthafecker", "muther", "mutherfucker", "muff", "ma5terb8", "ma5terbate", "masochist", "masturbate", "masterb8", "masterbat", "masterbat3", "masterbate", "masterbation", "masterbations"));
  private static final String mask = "***********************";

  /**
   * Comparator interface
   */
  private static Comparator<JsonObject> compareScore = new Comparator<JsonObject>() {
    @Override
    public int compare(JsonObject o1, JsonObject o2) {
      return o2.getDouble("value1").compareTo(o1.getDouble("value1"));
    }
  };
  private static Comparator<JsonObject> compareIS = new Comparator<JsonObject>() {
    @Override
    public int compare(JsonObject o1, JsonObject o2) {
      return o2.getLong("value1").compareTo(o1.getLong("value1"));
    }
  };
  private static Comparator<JsonObject> compareTID = new Comparator<JsonObject>() {
    @Override
    public int compare(JsonObject o1, JsonObject o2) {
      return o2.getLong("value2").compareTo(o1.getLong("value2"));
    }
  };
  private static Comparator<JsonObject> compareWORD = new Comparator<JsonObject>() {
    @Override
    public int compare(JsonObject o1, JsonObject o2) {
      return o2.getString("word").compareTo(o1.getString("word"));
    }
  };

  /**
   * Main program for extracting response from MySQL
   */
  public static String getResponse(List<JsonObject> resultSet, int n1, int n2) {
    // pre-calculate number of tweets returned
    int numRecords = resultSet.size();
    if (numRecords == 0) {
      return "";
    }
    // generate top n1 topic words, using PriorityQueue to implement "select-top-k" algorithm
    PriorityQueue<JsonObject> topicWordList = new PriorityQueue<JsonObject>(n1, compareScore.thenComparing(compareWORD));
    HashSet<String> topicWordSet = getTopicWords(n1, numRecords, topicWordList, resultSet);
    // generate top n2 tweets
    PriorityQueue<JsonObject> topTweetList = new PriorityQueue<JsonObject>(n2, compareIS.thenComparing(compareTID));
    getTopTweets(n2, topTweetList, resultSet, topicWordSet);
    // generate final response, note censoring output is done in the following function
    String response = generateOutput(topicWordList, topTweetList);
    return response;
  }

  /*
   * The methods below are the helper methods.
   *
   */

  /**
   * generate top n1 topic words
   */
  private static HashSet<String> getTopicWords(int n1, int N, PriorityQueue<JsonObject> pq, List<JsonObject> resultSet) {
    HashMap<String, Double> topicDF = new HashMap<String, Double>();
    // create dictionary to store all TF-IDF scores of all words
    for (JsonObject row: resultSet) {
      JsonObject TF_Object = new JsonObject(row.getString(TF));
      Set<String> words = TF_Object.fieldNames();
      for (String w: words) {
        topicDF.put(w, topicDF.getOrDefault(w, 0.0) + 1.0);
      }
      row.put(TF, TF_Object);
    }
    // calculate topic score
    HashMap<String, Double> topicScore = new HashMap<String, Double>();
    for (JsonObject row: resultSet) {
      JsonObject termfreq = row.getJsonObject(TF);
      Long impscore = row.getLong(IS);
      Set<String> words = termfreq.fieldNames();
      for (String word: words) {
        Double score = topicScore.getOrDefault(word, 0.0);
        Double IDF = Math.log(N/topicDF.get(word));
        Double TF = termfreq.getDouble(word);
        score += IDF * TF * Math.log(1+impscore);
        topicScore.put(word, score);
      }
    }
    for (String word: topicScore.keySet()){
      // add topic word, topic value pair to private list
      JsonObject obj = new JsonObject();
      obj.put("word", word);
      obj.put("value1", -1 * topicScore.get(word));
      // insert the word/value pair into a priority queue
      // maintain the size = n1
      if (pq.size() >= n1) {
        pq.offer(obj);
        pq.poll();
      } else {
        pq.offer(obj);
      }
    }
    // get topic words from priority queue and save as a set
    HashSet<String> topicWordSet = new HashSet<>();
    for (JsonObject p: pq) {
      topicWordSet.add(p.getString("word"));
    }
    return topicWordSet;
  }

  /**
   * generate top n2 tweets
   * @param n2
   * @param N
   */
  private static void getTopTweets(int n2, PriorityQueue<JsonObject> pq, List<JsonObject> resultSet,
                                   HashSet<String> topicWordSet) {
    Boolean found;
    // find all tweets that contain at least one topic word
    for (JsonObject row: resultSet){
      found = false;
      // select tweets that contain at least one topic word
      JsonObject termfreq = row.getJsonObject(TF);
      for (String w: termfreq.fieldNames()) {
        if (topicWordSet.contains(w)) {
          found = true;
          break;
        }
      }
      if (found) {
        Long ttid = row.getLong(ID);
        Long impscore = row.getLong(IS);
        String ttcen = row.getString(textCensor);
        JsonObject record = new JsonObject();
        record.put("value1", -1 * impscore);
        record.put("value2", -1 * ttid);
        record.put("tweet", ttcen);
        if (pq.size() >= n2) {
          pq.offer(record);
          pq.poll();
        } else {
          pq.offer(record);
        }
      }
    }
  }

  /**
   * generateOutput() - generate the desired output string from topicWordList and topTweetList
   * @return
   */
  private static String generateOutput(PriorityQueue<JsonObject> topicWordQueue,
                                       PriorityQueue<JsonObject> topTweetQueue) {
    String output = "";
    // output topic words in order first
    while (!topicWordQueue.isEmpty()) {
      JsonObject p = topicWordQueue.poll();
      // censor the topic word, if necessary
      String tword = p.getString("word");
      if (censored.contains(tword)) {
        tword = tword.substring(0, 1) +
          mask.substring(0, tword.length()-2) +
          tword.substring(tword.length()-1);
      }
      output = tword+":"+String.format("%.2f", -1 * p.getDouble("value1")) + "\t" + output;
    }
    output = output.substring(0, output.length()-1) + "\n"; // replace the last "tab" by new line
    // output top tweets by descending order in impact score and tweet id
    String tweetout = "";
    while (!topTweetQueue.isEmpty()){
      JsonObject record = topTweetQueue.poll();
      String temp = String.valueOf(-1 * record.getLong("value1")) + "\t" +
        String.valueOf(-1 * record.getLong("value2")) + "\t" +
        record.getString("tweet") + "\n";
      tweetout = temp + tweetout;
    }
    output = output + tweetout;
    return output.substring(0, output.length()-1);
  }
}
