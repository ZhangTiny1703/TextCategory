import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.ansj.domain.Result;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.yiciyuan.data.common.utils.CharacterHelper;

public class AutoClc5 {
    public static void main(String[] args) {
        BlockingQueue<Map<String, String>> blockingQueue1 = new ArrayBlockingQueue<>(10000);

        //启动进程
        Producer producer = new Producer(blockingQueue1);

        Thread queryThread = new Thread(producer);
        queryThread.setName("query");
        queryThread.start();
        for (int i = 0; i < 28; i++) {
            Consumer1 consumer1 = new Consumer1(blockingQueue1);
            Thread processThread = new Thread(consumer1);
            processThread.setName("process-" + i);
            processThread.start();
        }
    }

    public static class Producer implements Runnable {
        private final BlockingQueue<Map<String, String>> blockingQueue1;

        public Producer(BlockingQueue<Map<String, String>> blockingQueue1) {
            this.blockingQueue1 = blockingQueue1;
        }
        //数据库参数
        private static String driverName;
        private static String dbURL ;
        private static String userName;
        private static String userPwd ;
        private static Connection dbConn;
        private static Statement sm;
        private ResultSet rs;

        //静态块初始化加载，连接数据库
        static{
            try{
                readDBConfig();
                Class.forName(driverName);
                dbConn = DriverManager.getConnection(dbURL, userName, userPwd);
            }catch (Exception e){
                throw new RuntimeException("数据库连接失败！");
            }
        }

        //读取数据库配置文件
        private static void readDBConfig() throws IOException {
            InputStream is = AutoClc5.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            prop.load(is);
            driverName = prop.getProperty("DRIVERNAME1");
            dbURL = prop.getProperty("URL1");
            userName = prop.getProperty("USERNAME1");
            userPwd = prop.getProperty("PASSWORD1");

        }
        public void run() {
            DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            int i = 0;
            //连接数据库
            try {
                sm = dbConn.createStatement();
                System.out.println("producer connect successfully!");
            } catch (Exception e) {
                System.out.println("producer connect failed!");
                e.printStackTrace();
            }
            //取数据库放入队列
            try {
                rs = sm.executeQuery("select * from [DB_Fulltext_page].[dbo].[pages]");
                while (rs.next()) {
                    //System.out.println(rs.getString("content"));
                    Map<String, String> row = new HashMap<>();
                    row.put("id", rs.getString("docid"));
                    row.put("content", rs.getString("content"));
                    i += 1;
                    if (i % 1000 == 0) {
                        System.out.println(fmt.format(System.currentTimeMillis()) +"=> " + i);
                    }
                    blockingQueue1.put(row);
                }
                //查询结束标志
                Map<String, String> end = new HashMap<>();
                end.put("content", "end-end");
                blockingQueue1.put(end);
                System.out.println("query is over");

            } catch (Exception e) {
                System.out.println("query failed!");
                e.printStackTrace();
            }
        }
    }
    public static class Consumer1 implements Runnable {
        private final BlockingQueue<Map<String, String>> blockingQueue1;
        private volatile boolean flag = true;
        private static String driverName;
        private static String dbURL;
        private static String userName;
        private static String userPwd;
        private static Connection dbConn;
        private PreparedStatement sm;
//    private ResultSet rs = null;

        static{
            try{
                readDBConfig();
                Class.forName(driverName);
                dbConn = DriverManager.getConnection(dbURL, userName, userPwd);
                // dbConn.setAutoCommit(false);
            }catch (Exception e){
                throw new RuntimeException("数据库连接失败！");
            }
        }

        private static void readDBConfig() throws IOException {
            InputStream is = AutoClc5.class.getClassLoader().getResourceAsStream("db.properties");
            Properties prop = new Properties();
            prop.load(is);
            driverName = prop.getProperty("DRIVERNAME2");
            dbURL = prop.getProperty("URL2");
            userName = prop.getProperty("USERNAME2");
            userPwd = prop.getProperty("PASSWORD2");

        }

        public Consumer1(BlockingQueue<Map<String, String>> blockingQueue1) {
            this.blockingQueue1 = blockingQueue1;
        }

        private boolean isEnglishChar(char c) {
            if ((65 <= c && c <= 90) || (97 <= c && c <= 122)) {
                return true;
            }
            return false;
        }

        public String baseFormat(String text) {
            text = CharacterHelper.fullToHalf(text);
            text = text.replaceAll("\t+", " ");
            text = text.replaceAll("\f+", " ");
            text = text.replaceAll("\\v+", " ");
            text = text.replaceAll("\t+", " ");
            text = text.replaceAll(" +", " ");
            text = text.replaceAll(" +", " ");
            text = text.replaceAll("    +", " ");
            text = text.replaceAll("　+", " ");
            text = text.replaceAll("…+", " ");
            text = text.replaceAll("\\．{4,}", "．");
            text = text.replaceAll("\\.{4,}", ".");
            text = text.replaceAll("·+", " ");
            text = text.replaceAll("-+", "-");
            text = text.replaceAll("[,\\.\"'?/><\\*&^%$#@!`~;:|\\\\’‘]{3,}", " ");

            char[] arr = text.toCharArray();
            StringBuilder newText = new StringBuilder();
            for (int i = 0; i < arr.length; i++) {
                char c = arr[i];
                if (c == '★') {
                    c = ' ';
                }
                if (c == ' ') {
                    if (i == 0 || i == arr.length - 1) {
                        continue;
                    }
                    if (!isEnglishChar(arr[i - 1]) && !isEnglishChar(arr[i + 1])) {
                        continue;
                    }
                }
                newText.append(c);
            }

            return newText.toString().replaceAll("\r|\n", " ");

        }

        public void run() {
            DateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                sm = dbConn.prepareStatement("insert into [autoclcs1] (docid, content,clcs) values (?,?,?)");
                System.out.println("consumer connect successfully!");
            } catch (Exception e) {
                System.out.println("consumer connect failed!");
                e.printStackTrace();
            }
            int countn = 0;

            while (flag) {
                try {
                    Map<String, String> row = blockingQueue1.take();
                    String content = row.get("content");
                    String id = row.get("id");
                    //如果查询队列已取完
                    if (content.equals("end-end")) {
                        flag = false;
                        System.out.println("process is over");
                    } else {
                        content = baseFormat(content);
                        String newcontent = content;
                        int count = 0;        //count记录句数用来分段
                        int para_num = 0;
                        int begin = 0;        //段落的起始位置
                        int len = newcontent.length();      //原始数据长度

                        StringBuffer ToDB = new StringBuffer("{");
                        JSONArray arr = new JSONArray();
                        Map<String, String> map = new LinkedHashMap<>();
                        

                        //分段
                        for (int i = 0; i < len; i++) {
                            String subStr = newcontent.substring(i, i + 1);

                            if (";?!~。？！；".contains(subStr) || i == len - 1) {
                                if (5 == count || i == len - 1) {

                                    String Cutcontent = (newcontent.substring(begin, i + 1));
                                    para_num += 1;

                                    //分词
                                    Result result = ToAnalysis.parse(Cutcontent);
                                    List<Term> terms = result.getTerms();
                                    StringBuffer  paragraph = new StringBuffer();      //保存处理后的数据
                                    for (int j = 0; j < terms.size(); j++) {
                                        String word = terms.get(j).toString();
                                        word = word.split("/")[0];
                                        paragraph.append(word + " ");
                                    }
                                    String paragraph1 = paragraph.toString();
                                    paragraph1 = paragraph1.replaceAll("[[^\u4E00-\u9FA5]&&[^a-zA-Z0-9 ]]", "");

                                    //将所有分段存入数组
                                    map.put(paragraph1, para_num+":"+ "{" + begin + "," + i+1 + ",");
                                    
                                    begin = i + 1;
                                    count = 0;

                                    } else {
                                        count = count + 1;
                                    }
                                }
                            }
                        
                    //分类
                    PrintWriter out = null;
                    BufferedReader in = null;
                    StringBuilder result2 = new StringBuilder();

                    for (Iterator iter = map.keySet().iterator(); iter.hasNext(); ) {
                        arr.add(iter.next());
                    }
                    try {

                        URL url = new URL("http://192.168.2.8:6666/predict?k=3");
                        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                        conn.setRequestMethod("POST");
                        conn.setRequestProperty("accept", "*/*");
                        conn.setRequestProperty("connection", "Keep-Alive");
                        conn.setRequestProperty("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
                        conn.setDoOutput(true);
                        conn.setDoInput(true);
                        // 获取URLConnection对象对应的输出流
                        out = new PrintWriter(new OutputStreamWriter(conn.getOutputStream()));
                        // 发送请求参数
                        // arr = new JSONArray();
                        // paragraph = paragraph.replaceAll("[[^\u4E00-\u9FA5]&&[^a-zA-Z0-9 ]]", "");
                        //arr.add(paragraph);
                        //out.print(JSONObject.toJSONString(arr, SerializerFeature.BrowserCompatible));
                        out.print(arr);
                        // flush输出流的缓冲
                        out.flush();
                        // 定义BufferedReader输入流来读取URL的响应
                        in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                        String line;
                        while ((line = in.readLine()) != null) {
                            result2.append(line);
                        }

                    } catch (Exception e) {
//                                        e.printStackTrace();
                        result2.append("[[\"null\",\"null\",\"null\",\"null\",\"null\"],[1,1,1,1,1]]");
                    } finally {
                        if (in != null) {
                            try {
                                in.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        if (out != null) {
                            out.close();
                        }
                    }

                    ArrayList<StringBuffer> al = new ArrayList<>();
                    Pattern p = Pattern.compile("\\[\\[(.*?)\\],\\[(.*?)\\]\\]");
                    Matcher m = p.matcher(result2);
                    while (m.find()) {
                        StringBuffer result3 = new StringBuffer("{");
                        String[] s1 = m.group(1).split(",");
                        String[] s2 = m.group(2).split(",");

                        for (int k = 0; k < s1.length; k++) {
                            result3.append(s1[k] + ":" + s2[k] + ",");
                        }
                        result3.deleteCharAt(result3.length()-1);
                        result3.append("}");
                        al.add(result3);
                    }

                    int al_num = 0;
                    for (Iterator iter = map.keySet().iterator(); iter.hasNext();al_num++) {
                        Object key = iter.next();
                        ToDB.append(map.get(key)+ al.get(al_num)+",");
                    }
                    ToDB.deleteCharAt(ToDB.length()-1);
                    ToDB.append("}");
                        //写入数据库
                        try {
                            sm = dbConn.prepareStatement("insert into [autoclcs1] (docid,content,clcs ) values (?,?,?)");
                            sm.setObject(1, id);
                            sm.setObject(2, content);
                            sm.setObject(3, ToDB.toString());
                            sm.execute();

                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                } catch (Exception e) {
                    System.out.println("process failed!");
                    e.printStackTrace();
                }

            }

        }

    }
}

