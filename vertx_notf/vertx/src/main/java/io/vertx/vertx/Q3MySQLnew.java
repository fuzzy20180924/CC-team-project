package io.vertx.vertx;

import io.vertx.core.json.JsonObject;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Q3MySQLnew {
  /**
   *  The constructor.
   */
  public Q3MySQLnew() {

  }

  /**
   * MySQL table names
   */
  private static final String q3table = "q3notf";
  /**
   * MySQL table column names
   */
  private static final String uID = "user_id";
  private static final String text = "text";
  private static final String timestamp = "timestamp";
  private static final String ID = "id";
  private static final String IS = "impactScore";

  /**
   * Following is the hard-coded censor words set
   */
  private static final Set<String> censored = new HashSet<String>(Arrays.asList("15619cctest", "4r5e", "5h1t", "5hit", "n1gga", "n1gger", "nobhead", "nobjocky", "nobjokey", "nutsack", "numbnuts", "nazi", "nigg3r", "nigg4h", "nigga", "niggas", "niggaz", "niggah", "nigger", "niggers", "omg", "p0rn", "poop", "pron", "prick", "pricks", "pussy", "pussys", "pusse", "pussi", "pussies", "pube", "pawn", "penis", "penisfucker", "phonesex", "phuq", "phuck", "phuk", "phuks", "phuked", "phuking", "phukked", "phukking", "piss", "pissoff", "pisser", "pissers", "pisses", "pissflaps", "pissin", "pissing", "pigfucker", "pimpis", "queer", "rectum", "rimjaw", "rimming", "snatch", "sonofabitch", "spunk", "scrotum", "scrote", "scroat", "schlong", "sex", "semen", "sh1t", "shit", "shits", "shitty", "shitter", "shitters", "shitted", "shitting", "shittings", "shitdick", "shite", "shitey", "shited", "shitfuck", "shitfull", "shithead", "shiting", "shitings", "skank", "slut", "smut", "smegma", "tosser", "turd", "tw4t", "twunt", "twunter", "twat", "twatty", "twathead", "teets", "tit", "tittywank", "tittyfuck", "titties", "tittiefucker", "titwank", "titfuck", "v14gra", "v1gra", "vulva", "vagina", "viagra", "w00se", "wtff", "wang", "wank", "wanky", "wanker", "whore", "whore4r5e", "whoreshit", "whoreanal", "whoar", "a55", "anus", "anal", "arse", "ass", "assram", "asswhole", "assfucker", "assfukka", "assho", "b00bs", "b17ch", "b1tch", "boner", "booooooobs", "booooobs", "boooobs", "booobs", "boob", "boobs", "boiolas", "bollock", "bollok", "breasts", "bunnyfucker", "butt", "buttplug", "buttmuch", "buceta", "bugger", "bullshit", "bum", "bastard", "balls", "ballsack", "bestial", "bestiality", "beastial", "beastiality", "bellend", "bitch", "biatch", "bloody", "blowjob", "blowjobs", "c0ck", "c0cksucker", "cnut", "coon", "cox", "cock", "cocks", "cocksuck", "cocksucks", "cocksucker", "cocksucked", "cocksucking", "cocksuka", "cocksukka", "cockface", "cockhead", "cockmunch", "cockmuncher", "cok", "coksucka", "cokmuncher", "crap", "cunnilingus", "cunt", "cunts", "cuntlick", "cuntlicker", "cuntlicking", "cunilingus", "cunillingus", "cum", "cums", "cumshot", "cummer", "cumming", "cyalis", "cyberfuc", "cyberfuck", "cyberfucker", "cyberfuckers", "cyberfucked", "cyberfucking", "carpetmuncher", "cawk", "chink", "cipa", "cl1t", "clit", "clitoris", "clits", "d1ck", "donkeyribber", "doosh", "dogfucker", "doggin", "dogging", "duche", "dyke", "damn", "dink", "dinks", "dirsa", "dick", "dickhead", "dildo", "dildos", "dlck", "ejaculate", "ejaculates", "ejaculated", "ejaculating", "ejaculatings", "ejaculation", "ejakulate", "f4nny", "fook", "fooker", "fux", "fux0r", "fuck", "fucks", "fuckwhit", "fuckwit", "fucka", "fucker", "fuckers", "fucked", "fuckhead", "fuckheads", "fuckin", "fucking", "fuckings", "fuckingshitmotherfucker", "fuckkkdatttbitchhhh", "fuckme", "fudgepacker", "fuk", "fuks", "fukwhit", "fukwit", "fuker", "fukker", "fukkin", "fannyfucker", "fannyflaps", "fanyy", "fag", "fagot", "fagots", "fags", "faggot", "faggs", "fagging", "faggitt", "fcuk", "fcuker", "fcuking", "feck", "fecker", "felching", "fellate", "fellatio", "fingerfuck", "fingerfucks", "fingerfucker", "fingerfuckers", "fingerfucked", "fingerfucking", "fistfuck", "fistfucks", "fistfucker", "fistfuckers", "fistfucked", "fistfucking", "fistfuckings", "flange", "goatse", "goddamn", "gangbang", "gangbangs", "gangbanged", "gaysex", "horny", "horniest", "hore", "hotsex", "hoar", "hoare", "hoer", "homo", "hardcoresex", "heshe", "hell", "jap", "jackoff", "jerk", "jerkoff", "jism", "jiz", "jizz", "jizm", "knob", "knobend", "knobead", "knobed", "knobhead", "knobjocky", "knobjokey", "kondum", "kondums", "kock", "kunilingus", "kum", "kums", "kummer", "kumming", "kawk", "l3itch", "l3ich", "lust", "lusting", "labia", "lmao", "lmfao", "m0f0", "m0fo", "m45terbate", "mothafuck", "mothafucks", "mothafucka", "mothafuckas", "mothafuckaz", "mothafucker", "mothafuckers", "mothafucked", "mothafuckin", "mothafucking", "mothafuckings", "motherfuck", "motherfucks", "motherfucker", "motherfuckers", "motherfucked", "motherfuckin", "motherfucking", "motherfuckings", "motherfuckka", "mof0", "mofo", "mutha", "muthafuckker", "muthafecker", "muther", "mutherfucker", "muff", "ma5terb8", "ma5terbate", "masochist", "masturbate", "masterb8", "masterbat", "masterbat3", "masterbate", "masterbation", "masterbations"));
  private static final Set<String> stopwords = new HashSet<>(Arrays.asList("a", "about", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "amoungst", "amount", "an", "and", "another", "any", "anyhow", "anyone", "anything", "anyway", "anywhere", "are", "aren't", "around", "as", "at", "back", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom", "but", "by", "call", "can", "cannot", "cant", "can't", "co", "computer", "con", "could", "couldnt", "couldn't", "cry", "de", "describe", "detail", "didn't", "do", "doesn't", "done", "don't", "down", "due", "during", "each", "eg", "eight", "either", "eleven", "else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "hadn't", "has", "hasnt", "hasn't", "have", "haven't", "he", "he'd", "he'll", "hence", "her", "here", "hereafter", "hereby", "herein", "here's", "hereupon", "hers", "herself", "he's", "him", "himself", "his", "how", "however", "how's", "hundred", "i", "i'd", "ie", "if", "i'll", "i'm", "in", "inc", "indeed", "interest", "into", "is", "isn't", "it", "its", "it's", "itself", "i've", "keep", "last", "latter", "latterly", "least", "less", "let's", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "mustn't", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own", "part", "per", "perhaps", "please", "put", "rather", "re", "rt", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "that's", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "there's", "thereupon", "these", "they", "they'd", "they'll", "they're", "they've", "thick", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "wasn't", "we", "we'd", "well", "we'll", "were", "we're", "weren't", "we've", "what", "whatever", "what's", "when", "whence", "whenever", "when's", "where", "whereafter", "whereas", "whereby", "wherein", "where's", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "who's", "whose", "why", "why's", "will", "with", "within", "without", "won't", "would", "wouldn't", "yet", "you", "you'd", "you'll", "your", "you're", "yours", "yourself", "yourselves", "you've"));
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
      return o2.getLong("value1").compareTo(o1.getLong("value2"));
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
    // generate top n1 topic words
    System.out.println("in Q3MySQLnew class "+String.valueOf(numRecords));
    PriorityQueue<JsonObject> topicWordList = new PriorityQueue<JsonObject>(n1, compareScore.thenComparing(compareWORD));
    LinkedList<HashMap<String, Double>> termFreqList = getTopicWords(n1, numRecords, topicWordList, resultSet);
    System.out.println("topic words generated");
    // generate top n2 tweets
    HashSet<String> topicWordSet = new HashSet<String>();
    for (JsonObject w: topicWordList) {
	topicWordSet.add(w.getString("word"));
    }
    PriorityQueue<JsonObject> topTweetList = new PriorityQueue<JsonObject>(n2, compareIS.thenComparing(compareTID));
    getTopTweets(n2, topTweetList, termFreqList, resultSet, topicWordSet);
    System.out.println("top tweets selected");
    // generate Output
    String response = generateOutput(topicWordList, topTweetList);
    System.out.println("output response generated.");
    return response;
  }

  /**
   * get topic words and calculate term frequency
   */
  private static void calculateTF(HashMap<String, Double> termFreq, String tweetText) {
    String urlReg = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
    String[] splitted = tweetText.replaceAll(urlReg, "").split("[^a-zA-Z0-9'-]+");
    int count = 0;
    // filter non-words
    for (String word: splitted){
      // if non-word continue
      if (!word.matches(".*[a-zA-Z]+.*")) {
        continue;
      } else {
        // increment word count
        count++;
        // record only topic words
        String w = word.toLowerCase();
        if (!stopwords.contains(w)) {
          termFreq.put(w, termFreq.getOrDefault(w, 0.0)+1.0);
        }
      }
    }
    // calculate term frequency
    for (String w: termFreq.keySet()){
      termFreq.put(w, termFreq.get(w)/count);
    }
  }

  /**
   * generate top n1 topic words
   */
  private static LinkedList<HashMap<String, Double>> getTopicWords(int n1, int N, PriorityQueue<JsonObject> pq, 
		  						   List<JsonObject> resultSet) {
    HashMap<String, Double> topicDF = new HashMap<String, Double>();
    LinkedList<HashMap<String, Double>> termFreqList = new LinkedList<>();
    HashMap<String, Double> topicScore = new HashMap<String, Double>();
    // create dictionary to store all TF-IDF scores of all words
    for (JsonObject row: resultSet) {
      HashMap<String, Double> termFreq = new HashMap<>();
      calculateTF(termFreq, row.getString(text));
      termFreqList.add(termFreq);
      Long impscore = row.getLong(IS);
      for (String w: termFreq.keySet()) {
        topicDF.put(w, topicDF.getOrDefault(w, 0.0) + 1.0);
        Double score = topicScore.getOrDefault(w, 0.0);
        Double TF = termFreq.get(w);
        score += TF * Math.log(1+impscore);
        topicScore.put(w, score);
      }
    }
    for (String word: topicScore.keySet()){
      JsonObject obj = new JsonObject();
      obj.put("word", word);
      Double value = -1 * topicScore.get(word) * Math.log(Double.valueOf(N)/topicDF.get(word)); // N-1 hardcode correction
      obj.put("value1", value);
      // insert the word/value pair into a priority queue
      // maintain the size = n1
      if (pq.size() >= n1) {
        pq.offer(obj);
        pq.poll();
      } else {
        pq.offer(obj);
      }
    }
    
    return termFreqList;
  }

  /**
   * generate top n2 tweets
   * @param n2
   * @param N
   */
  private static void getTopTweets(int n2, PriorityQueue<JsonObject> pq,
                                   LinkedList<HashMap<String, Double>> termFreqList,
				   List<JsonObject> resultSet,
				   HashSet<String> topicWordSet) {
    Boolean found;
    Iterator<HashMap<String, Double>> tfIter = termFreqList.iterator();
    // find all tweets that contain at least one topic word
    for (JsonObject row: resultSet){
      found = false;
      HashMap<String, Double> termfreq = tfIter.next();
      // split the tweet into words first
      for (String w: termfreq.keySet()) {
        if (topicWordSet.contains(w)) {
          found = true;
          break;
        }
      }
      if (found) {
        Long ttid = row.getLong(ID);
        Long impscore = row.getLong(IS);
        String ttcen = row.getString(text);
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
   * maskText(record.getString("tweet"))
   */
  private static String maskText(String origText) {
    // first split the tweet into whole words
    // following same logic as in calculation of term frequency
    String urlReg = "(https?|ftp)://[^\\t\\r\\n /$.?#][^\\t\\r\\n ]*";
    String[] splitted = origText.replaceAll(urlReg, "").split("[^a-zA-Z0-9'-]+");
    String censText = origText;
    // This list will contain whole word matches
    List<String> matchedPatterns = new LinkedList<>();
    // second, iterate through all words
    for (String word: splitted){
      // check if the word (lower case) is a censored word
      if (censored.contains(word.toLowerCase())){
        String cword = word.substring(0, 1) + mask.substring(0, word.length()-2) + word.substring(word.length()-1);
        // edge case 1: tweet is just one word
        if (censText.equals(word)) {
          return cword;
        }
        matchedPatterns.clear();
        // find all whole word patterns in the tweet
        Matcher m = Pattern.compile("[^a-zA-Z0-9'-]" + word + "[^a-zA-Z0-9'-]").matcher(censText);
        while (m.find()) {
          matchedPatterns.add(m.group());
        }
        // replace each pattern in the tweet
        for (String pattern: matchedPatterns){
          String patternRep = pattern.replaceFirst(word, cword);
          censText = censText.replaceAll(pattern, patternRep);
        }
        // edge case 2: censored word starts at beginning
        String edgeCase = word + "[^a-zA-Z0-9'-]";
        if (censText.substring(0, word.length()+1).matches(edgeCase)) {
          censText = censText.replaceFirst(word, cword);
        }
        // edge case 3: censored word appears at the end
        edgeCase = "[^a-zA-Z0-9'-]" + word;
        if (censText.substring(censText.length()-word.length()-1).matches(edgeCase)) {
          censText = censText.substring(0, censText.length()-word.length()) + cword;
        }
      }
    }

    return censText;
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
      output = tword + ":" + String.format("%.2f", -1 * p.getDouble("value1")) + "\t" + output;
    }
    output = output.substring(0, output.length()-1) + "\n"; // replace the last "tab" by new line
    // output top tweets by descending order in impact score and tweet id
    String tweetout = "";
    while (!topTweetQueue.isEmpty()){
      JsonObject record = topTweetQueue.poll();
      String censoredText = maskText(record.getString("tweet"));
      String temp = String.valueOf(-1 * record.getLong("value1")) + "\t" +
        String.valueOf(-1 * record.getLong("value2")) + "\t" +
        censoredText + "\n";
      tweetout = temp + tweetout;
    }
    output = output + tweetout;
    return output.substring(0, output.length()-1);
  }
}

