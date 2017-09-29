//creats a sends relationship only if receiver is also a sender
package exampletutorialneo4j;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;

public class Neo4jJava {

	public enum Relationships implements RelationshipType{
		SENDS,RECEIVES;
	}
	public enum Email implements Label {
		SENDER,RECEIVER;
	}
	public static Map<String, Integer> mapsender = new LinkedHashMap<String,Integer>();
	public static Map<String, Integer> mapreceiver = new LinkedHashMap<String,Integer>();
	public static Map<String,ArrayList<String>> maplist = new LinkedHashMap<String,ArrayList<String>>();
	public static File Neo4j_DB = new File("C:/TPNeo4jDB");
	public static GraphDatabaseFactory dbFactory = new GraphDatabaseFactory();
	public static GraphDatabaseService db= dbFactory.newEmbeddedDatabase(Neo4j_DB);
	public static IndexManager index = db.index();
	public static Node initialNode=null;
	public static ArrayList<Node> visitednodes = new ArrayList<Node>();
	public static Map<Integer,ArrayList<Node>> finalnodes = new LinkedHashMap<Integer,ArrayList<Node>>();

public static void main(String[] args) throws IOException {
	try(Transaction transaction = db.beginTx())
    {
	Index<Node> createindex = index.forNodes( "createindex" );
	int increment = 1; String sender="";
	URL oracle = new URL("http://www.cs.uky.edu/~marek/dataset");
    URLConnection yc = oracle.openConnection();
    BufferedReader in = new BufferedReader(new InputStreamReader(yc.getInputStream()));
    String inputLine;
    while ((inputLine = in.readLine()) != null){
    	int trackingthesender =0;
    	ArrayList<String> receivers = new ArrayList<String>();
        Scanner input = new Scanner(inputLine);
        while(input.hasNext()){
        	String token = input.next();
        	if(trackingthesender==0){
        		sender = token;
        		if(mapsender.containsValue(sender)){
        			trackingthesender++;
        		}
        		else{
        			Node node1 = db.createNode(Email.SENDER);
            	    node1.setProperty("name", token);
            	    createindex.add(node1, "name", node1.getProperty("name"));
        		    mapsender.put(sender,increment);
        		    increment++;
        		    trackingthesender++;
        		}
        	}		
        	else{
        		   receivers.add(token);
        	}
        	maplist.put(sender, receivers);
        }
    } 
    in.close();
    
    URL neworacle = new URL("http://www.cs.uky.edu/~marek/dataset");
    URLConnection newyc = neworacle.openConnection();
    BufferedReader newin = new BufferedReader(new InputStreamReader(newyc.getInputStream()));
    String inputLines;
    System.out.println("Creating Nodes");
    while ((inputLines = newin.readLine()) != null){
    	int trackingthesender =0;
        Scanner input = new Scanner(inputLines);
        while(input.hasNext()){
        	String token = input.next();
        	if(trackingthesender==0){
        		  trackingthesender++;
        		}		
        	else{
        		if(!mapsender.containsKey(token)){
        		if(!mapreceiver.containsKey(token))
        		{
        		Node node1 = db.createNode(Email.SENDER);
        	    node1.setProperty("name", token);
        	    createindex.add(node1, "name", node1.getProperty("name"));
    		    mapreceiver.put(sender,increment); 
    		    } }
        	}
        }
    } 
    in.close();
    
	System.out.println("\n Nodes created successfully");
	int count_of_relationships=0;
	
	 for(Map.Entry<String, ArrayList<String>> mapping: maplist.entrySet()){
		Node node1= null;
		if(mapsender.containsKey(mapping.getKey())){
			IndexHits<Node> hits1;
    		hits1 = createindex.get("name", mapping.getKey());
    		  for (Node n : hits1) {
    		    if (n.getProperty("name").toString().equalsIgnoreCase(mapping.getKey())) {
    		           node1 = n;
    		    } 
    		  }
		}
		
		List<String> values = mapping.getValue();
		for (int i = 0; i < values.size(); i++) {
			   Node node2 = null;
        	   if(mapsender.containsKey(values.get(i))){
        		IndexHits<Node> hits2;
           		hits2 = createindex.get("name", values.get(i));
           		  for (Node n : hits2) {
           		    if (n.getProperty("name").toString().equalsIgnoreCase(values.get(i))) {
           		           node2 = n;
           		    } 
           		  }
             }

        	   if(node1 != null && node2!=null){
        		   if(node1==node2){
        			   //ignore self loops
        		   }
        		   else{
        			   Relationship relationship1 = node1.createRelationshipTo(node2,Relationships.SENDS);
       			       count_of_relationships++; 
       			       }
              }
		}
		 
	}
	 for(Map.Entry<String,Integer> list: mapsender.entrySet()){
			Node node2= null;
			if(list.getKey().equals("WOODS")){
		    IndexHits<Node> hits2;
			hits2 = createindex.get("name", list.getKey());
			  for (Node n : hits2) {
			    if (n.getProperty("name").toString().equalsIgnoreCase(list.getKey())) {
			           node2 = n;
			    }
			  }initialNode = node2; 
			}
	 }
	 for(Map.Entry<String,Integer> list: mapsender.entrySet()){
		Node node2= null;
		if(list.getKey().equals("SMITH")){
	    IndexHits<Node> hits2;
		hits2 = createindex.get("name", list.getKey());
		  for (Node n : hits2) {
		    if (n.getProperty("name").toString().equalsIgnoreCase(list.getKey())) {
		           node2 = n;
		    }
		  } 
   
		
		Map<Integer,ArrayList<Node>> finalcomponents = recursion(node2,visitednodes);
		if(finalcomponents.isEmpty())
			System.out.println("\n There is no path");
		else{
	   	System.out.println("Connected Components for " + initialNode.getProperty("name") + " are ");
		for(Map.Entry<Integer, ArrayList<Node>> entry : finalcomponents.entrySet()){
         System.out.print(" \n Path " + entry.getKey() +": " + entry.getValue().size());
         for(int s=0; s<entry.getValue().size();s++){
         Node n1 = entry.getValue().get(s);
         System.out.print( n1.getProperty("name") + " , " );
 		  } 
         } System.out.println("\n No of Paths: " + finalcomponents.size()); 
		
		 System.out.println("The connected nodes are " );
 		   connectedcomponents.remove(initialNode);
         for(Map.Entry<Integer, Node> entry : connectedcomponents.entrySet()){
        	 int i=0;
        	 System.out.print(i + " : ");
        	 Node n1 = entry.getValue();
	         System.out.println( n1.getProperty("name"));
	         i++;
         } System.out.println("Size " + connectedcomponents.size()); 
 } } }
		transaction.success();
 } 
}

static int i =0; static int k=0; static int maxpath=0;
public static Map<Integer,Node> connectedcomponents = new LinkedHashMap<Integer,Node>();
public static Map<Integer,ArrayList<Node>> recursion(Node currentnode, ArrayList<Node> visitednodes){
	ArrayList<Node> localnode = (ArrayList<Node>)visitednodes.clone();
	visitednodes = localnode;
	visitednodes.add(currentnode);
	boolean answer = false;
	final Iterable<Relationship> relationships = currentnode.getRelationships(Direction.OUTGOING, Relationships.SENDS);
 Node attachednode;
 
    for( Relationship relationship : relationships )
    {
		attachednode = relationship.getOtherNode(currentnode);
	    if(visitednodes.size() >=3){
	    	answer = matching(visitednodes);
	    }
		    
		if(visitednodes.size()==1){
			k++;
		   }
		else if(visitednodes.size() > 3 ){
			return finalnodes;
		}
		else if(currentnode.equals(initialNode)){
		   ArrayList<Node> updatedlist = (ArrayList<Node>)visitednodes.clone();
		   i=i+1;
		   finalnodes.put(i, updatedlist);
		   for(int f=0; f<visitednodes.size();f++){
		       if(!(connectedcomponents.containsValue(visitednodes.get(f))))
			   connectedcomponents.put(++i, visitednodes.get(f));
		      }
		   return finalnodes;
	       } 
		else{
			if(answer == true){
		    	
				return finalnodes;
			    }
		}
	     recursion(attachednode, visitednodes);
  }
   return finalnodes;
	
}

public static boolean matching(ArrayList<Node> visitednodes){
	for(int i=0; i<visitednodes.size()-1;i++){
		Node N1 = visitednodes.get(i);
		Node N2 = visitednodes.get(visitednodes.size()-1);
		if(N1.toString().equals(N2.toString())){
			return true;
		}
	}
	return false; 
 }
}
