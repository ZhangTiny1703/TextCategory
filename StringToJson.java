import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 1.string转json的形式有很多，但是一定要注意string里的json格式一定要正确，不然不会解析出来
 * 2.json转string，调用toString()方法
 * 3.引用的jar包，org.json.JSONObject和com.alibaba.fastjson.JSONObject同时存在会有冲突，如果没有注明引用的哪个包
 */
public class test1{
    public static void main(String[] args) {
        //string字符串转json
        String s = "id:100,"+
                "name:tiny"+
                "info:{city:wuhan,favorite:eating,data:lazy}"+
                "{1:[0,4461,{\"wuxiandian\":0.96637046,\"hangyejingji\":0.02644726,\"zidonghuajisuanji\":0.00663271}]," +
                "0:[0,4501,{\"lishi\":0.99957985,\"wenhua\":0.00044014349,\"gongchandang\":0.000010042532}]}";

        //转换成json对象，使用org.json的jar包
        JSONObject jstr = new JSONObject(s);

        //获取int型数据
        int cat1 = jstr.getInt("id");

        //获取String型数据
        String cat2 = jstr.getString("name");

        //获取JSONObject型数据
        JSONObject info = jstr.getJSONObject("info");
        System.out.println(info.getString("city"));
        
        //获取JSONArray数组
        JSONArray jarr = jstr.getJSONArray("1"); 
        

    }
}
