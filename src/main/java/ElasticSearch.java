import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

/**
 * Created by rohan on 5/30/16.
 */
public class ElasticSearch {

    public static Map<String, Object> putJsonDocument(String movieName, String directorName){
        Map<String, Object> jsonDocument = new HashMap<String, Object>();
        jsonDocument.put("movieName", movieName);
        jsonDocument.put("directorName", directorName);
        return jsonDocument;
    }

    public static void indexOneNewDocument(Client client,String movieName,String directorName,int index){
        client.prepareIndex("movies", "movie", String.valueOf(index))
                .setSource(putJsonDocument(movieName,directorName)).execute().actionGet();

    }

    public static void searchDocumentsByField(Client client, String index, String type,
                                      String field, String value){

        SearchResponse response = client.prepareSearch(index)
                .setTypes(type)
                .setSearchType(SearchType.QUERY_AND_FETCH)
                .setQuery(fieldQuery(field, value))
                .setFrom(0).setSize(60).setExplain(true)
                .execute()
                .actionGet();

        SearchHit[] results = response.getHits().getHits();
        System.out.println("Current results: " + results.length);
        for (SearchHit hit : results) {
            System.out.println();
            Map<String,Object> result = hit.getSource();
            System.out.println(result);
        }
    }

    public static void updateDocument(Client client, String index, String type,
                                      String id, String field, String newValue){
        Map<String, Object> updateObject = new HashMap<String, Object>();
        updateObject.put(field, newValue);
        client.prepareUpdate(index, type, id)
                .setScript("ctx._source." + field + "=" + field)
                .setScriptParams(updateObject).execute().actionGet();
    }

    public static void deleteDocument(Client client, String index, String type, String id){
        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
        System.out.println("Information on the deleted document:");
        System.out.println("Index: " + response.getIndex());
        System.out.println("Type: " + response.getType());
        System.out.println("Id: " + response.getId());
        System.out.println("Version: " + response.getVersion());
    }

    public static void main(String[] args){
        Node node  = nodeBuilder().clusterName("MovieCluster").node();
        Client client = node.client();

        // Indexing 5 documents
        indexOneNewDocument(client,"Interstellar","Christopher Nolan",1);
        indexOneNewDocument(client,"Jurassic Park","Stephen Speilberg",2);
        indexOneNewDocument(client,"Avatar","James Cameron",3);
        indexOneNewDocument(client,"Inception","Christopher Nolan",4);
        indexOneNewDocument(client,"Slumdog Millionaire","Danny Boyle",5);

        // Search documents by ID
        System.out.println("--------------Search By ID----------------");
        for(int i=1;i<6;i++) {
            GetResponse getResponse = client.prepareGet("movies", "movie", String.valueOf(i)).execute().actionGet();
            Map<String, Object> source = getResponse.getSource();
            System.out.println("Id: " + getResponse.getId());
            System.out.println(source);
            System.out.println();
        }

        // Search movies by Directors
        System.out.println("-------------Search By Director-----------");

        searchDocumentsByField(client,"movies","movie","directorName","Christopher Nolan");

        // Update a document
        updateDocument(client,"movies","movie","2","directorName","Rohan Kulkarni");
        GetResponse getResponse = client.prepareGet("movies", "movie", "2").execute().actionGet();
        Map<String, Object> source = getResponse.getSource();
        System.out.println("Id: " + getResponse.getId());
        System.out.println(source);
        System.out.println();

        // Delete a document
        deleteDocument(client,"movies","movie","2");
        node.close();

    }
}
