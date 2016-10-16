package net.thornydev;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class JsonHiveSchema  {
  
  static void help() {
    System.out.println("Usage: Two arguments possible. First is required. Second is optional");
    System.out.println("  1st arg: path to JSON file to parse into Hive schema");
    System.out.println("  2nd arg (optional): tablename.  Defaults to 'x'");
  }
  
  public static void main( String[] args ) throws Exception {
  /*  if (args.length == 0) {
      throw new IllegalArgumentException("ERROR: No file specified");
    }
    
    if (args[0].equals("-h")) {
      help();
      System.exit(0);
    }*/
    
    StringBuilder sb = new StringBuilder();
    BufferedReader br = new BufferedReader( new FileReader("C:\\Rashnil\\UNC\\Study\\Fall2016\\AWS Hadoop\\AWS\\AltmetricDataProcessing\\Java\\json-java-processing\\target\\sample.json") );
    String line;
    while ( (line = br.readLine()) != null ) {
      sb.append(line).append("\n");
    }
    br.close();
    
    String tableName = "x";
    if (args.length == 2) {
      tableName = args[1];
    }

    JsonHiveSchema schemaWriter = new JsonHiveSchema(tableName);
    System.out.println(schemaWriter.createHiveSchema(sb.toString()));
  }
  
  
  private String tableName = "x";
  
  
  public JsonHiveSchema() {}
  
  public JsonHiveSchema(String tableName) {
    this.tableName = tableName;
  }
  
  /**
   * Pass in any valid JSON object and a Hive schema will be returned for it.
   * You should avoid having null values in the JSON document, however.
   * 
   * The Hive schema columns will be printed in alphabetical order - overall and
   * within subsections.
   * 
   * @param json
   * @return string Hive schema
   * @throws JSONException if the JSON does not parse correctly
   */
  public String createHiveSchema(String json) throws JSONException {
    JSONObject jo = new JSONObject(json);
    
    @SuppressWarnings("unchecked")
    Iterator<String> keys = jo.keys();
    keys = new OrderedIterator(keys);
    StringBuilder sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (\n");

    while (keys.hasNext()) {
      String k = keys.next();
      sb.append("  ");
      sb.append(k.toString());
      sb.append(' ');
      sb.append(valueToHiveSchema(jo.opt(k)));
      sb.append(',').append("\n");
    }

    sb.replace(sb.length() - 2, sb.length(), ")\n"); // remove last comma
    return sb.append("ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe';").toString();
  }

  private String toHiveSchema(JSONObject o) throws JSONException { 
    @SuppressWarnings("unchecked")
    Iterator<String> keys = o.keys();
    keys = new OrderedIterator(keys);
    StringBuilder sb = new StringBuilder("struct<");
    
    while (keys.hasNext()) {
      String k = keys.next();
      sb.append(k.toString());
      sb.append(':');
      sb.append(valueToHiveSchema(o.opt(k)));
      sb.append(", ");
    }
    sb.replace(sb.length() - 2, sb.length(), ">"); // remove last comma
    return sb.toString();
  }

  private String toHiveSchema(JSONArray a) throws JSONException {
    return "array<" + arrayJoin(a, ",") + '>';
  }

  private String arrayJoin(JSONArray a, String separator) throws JSONException {
    StringBuilder sb = new StringBuilder();

    if (a.length() == 0) {
      throw new IllegalStateException("Array is empty: " + a.toString());
    }
    
    Object entry0 = a.get(0);
    if ( isScalar(entry0) ) {
      sb.append( scalarType(entry0) );
    } else if (entry0 instanceof JSONObject) {
      sb.append( toHiveSchema((JSONObject)entry0) );
    } else if (entry0 instanceof JSONArray) {    
      sb.append( toHiveSchema((JSONArray)entry0) );
    }
    return sb.toString();
  }
  
  private String scalarType(Object o) {
    if (o instanceof String) return "string";
    if (o instanceof Number) return scalarNumericType(o);
    if (o instanceof Boolean) return "boolean";
    return null;
  }

  private String scalarNumericType(Object o) {
    String s = o.toString();
    if (s.indexOf('.') > 0) {
      return "double";
    } else {
      return "int";
    }
  }

  private boolean isScalar(Object o) {
    return o instanceof String ||
        o instanceof Number ||
        o instanceof Boolean || 
        o == JSONObject.NULL;
  }

  private String valueToHiveSchema(Object o) throws JSONException {
    if ( isScalar(o) ) {
      return scalarType(o);
    } else if (o instanceof JSONObject) {
      return toHiveSchema((JSONObject)o);
    } else if (o instanceof JSONArray) {
      return toHiveSchema((JSONArray)o);
    } else {
      throw new IllegalArgumentException("unknown type: " + o.getClass());
    }
  }
  
  static class OrderedIterator implements Iterator<String> {

    Iterator<String> it;
    
    public OrderedIterator(Iterator<String> iter) {
      SortedSet<String> keys = new TreeSet<String>();
      while (iter.hasNext()) {
        keys.add(iter.next());
      }
      it = keys.iterator();
    }
    
    public boolean hasNext() {
      return it.hasNext();
    }

    public String next() {
      return it.next();
    }

    public void remove() {
      it.remove();
    }
  }
}
