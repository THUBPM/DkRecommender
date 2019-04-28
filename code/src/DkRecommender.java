package recommender;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVReader;

public class D2Recommendation {
	// This file reads the relation sets when k = 1, then it extracts the relation sets when k = 1 to gram+1 and doing recommendation. 

	static String inputRoot = ".\\bigRealConcurrent\\";
	static String outputRoot = ".\\log\\";
	
	static String entityIdFile = inputRoot + "entity2id.txt";
	static String relationIdFile = inputRoot + "relation2id.txt";
	static String graphIdFile = inputRoot + "graph2id.txt";
	
	static String trainFile = inputRoot + "train.txt";
	static String testFile = inputRoot + "test.txt";
	static String[] fileList = {inputRoot + "S0.txt", inputRoot + "S1.txt", inputRoot + "S2.txt", inputRoot + "S3.txt", inputRoot + "S4.txt"};
	//static String validFile = inputRoot + "valid.txt";
	
	static String logString = outputRoot + "big_real_concurrent";
	static int gram = 4; // max(k) - 1
	static boolean fiveFold = false;
	
	static String encodingText = "utf-8";
	static char separator = '\t';
	
	static public class NumWithIndex {
		int num = 0;
		int index = -1;
		
		public NumWithIndex(int num, int index) {
			this.num = num;
			this.index = index;
		}
	}
	
	static public class ComparatorNumWithIndexNum implements Comparator<NumWithIndex>{
        public int compare(NumWithIndex item1, NumWithIndex item2) {
        	if(item1 == null && item2 == null) {return 0;}
        	else if(item2 == null) {return -1;}
        	else if(item1 == null) {return 1;}
        	else if(item1.num < item2.num) {return 1;}
            else if (item1.num > item2.num) {return -1;}
            else {return 0;}
        }
    }
	
	static HashMap<String, Integer> entityId = new HashMap<String, Integer>();
	static HashMap<String, Integer> relationId = new HashMap<String, Integer>();
	static HashMap<String, Integer> graphId = new HashMap<String, Integer>();
	static HashMap<Integer, String> entityString = new HashMap<Integer, String>();
	static HashMap<Integer, String> relationString = new HashMap<Integer, String>();
	static HashMap<Integer, String> graphString = new HashMap<Integer, String>();
	static int entityCount = 0;
	static int relationCount = 0;
	static int graphCount = 0;
	
	//static ArrayList<int[]> trainList = new ArrayList<int[]>();
	static ArrayList<int[]> testList = new ArrayList<int[]>();
	//static ArrayList<int[]> validList = new ArrayList<int[]>();
	static HashMap<Integer, HashMap<String, ArrayList<NumWithIndex>>> trainMap = new HashMap<Integer, HashMap<String, ArrayList<NumWithIndex>>>();
	static HashMap<String, Boolean> testTripleMap = new HashMap<String, Boolean>();
	static HashMap<String, Integer> testCountMap = new HashMap<String, Integer>();
	
	static int trainCount = 0;
	static int testCount = 0;
	//static int validCount = 0;
	
	static int[] rCount = new int[11];
	static int[] tCount = new int[11];
	static int[] rtCount = new int[11];
	static int[] hitCount = new int[11];
	static int[][] trueCount;
	static int[] totalCount;
	
	static FileWriter fw = null;
	static BufferedWriter bw = null;
	
	public static void main(String[] args) throws IOException {
		if(fiveFold) {
			for(int g = 0; g <= gram; g++) {
				for(int f = 0; f < 5; f++) {
					init();
					String logFile = logString + "_" + f + "_g" + g + ".txt";
					
					fw = new FileWriter(logFile);
					fw.close();
					
			    	fw = new FileWriter(logFile, true);
			        bw = new BufferedWriter(fw);
					
					entityCount = ReadId(entityIdFile, entityId, entityString);
					relationCount = ReadId(relationIdFile, relationId, relationString);
					graphCount = ReadId(graphIdFile, graphId, graphString);
					
					for(int i = 0; i <= gram; i++) {
						HashMap<String, ArrayList<NumWithIndex>> trainMapK = new HashMap<String, ArrayList<NumWithIndex>>();
						trainMap.put(i, trainMapK);
					}

					for(int i = 0; i < 5; i++) {
						if(i != f)
							trainCount = ReadTrain(g, fileList[i], trainMap);
						else
							testCount = ReadTest(fileList[i], testList, testTripleMap, testCountMap);
					}
					
					trueCount = new int[graphCount][11];
					totalCount = new int[graphCount];
					
					Evaluate(g, trainMap);
					WriteResult();
					
				    bw.flush();
				    bw.close();
				    fw.close();
				}
			}
		}
		else {
			for(int g = 0; g <= gram; g++) {
				init();
				String logFile = logString + "_g" + g + ".txt";
				
				fw = new FileWriter(logFile);
				fw.close();
				
		    	fw = new FileWriter(logFile, true);
		        bw = new BufferedWriter(fw);
				
				entityCount = ReadId(entityIdFile, entityId, entityString);
				relationCount = ReadId(relationIdFile, relationId, relationString);
				graphCount = ReadId(graphIdFile, graphId, graphString);
				
				for(int i = 0; i <= gram; i++) {
					HashMap<String, ArrayList<NumWithIndex>> trainMapK = new HashMap<String, ArrayList<NumWithIndex>>();
					trainMap.put(i, trainMapK);
				}
				
				trainCount = ReadTrain(g, trainFile, trainMap);
				testCount = ReadTest(testFile, testList, testTripleMap, testCountMap);
				
				trueCount = new int[graphCount][11];
				totalCount = new int[graphCount];
				
				Evaluate(g, trainMap);
				WriteResult();
				
			    bw.flush();
			    bw.close();
			    fw.close();
			}
		}
	}
	
	public static void init() {
		entityId.clear();
		relationId.clear();
		graphId.clear();
		entityString.clear();
		graphString.clear();
		entityCount = 0;
		relationCount = 0;
		graphCount = 0;
		
		//trainList.clear();
		testList.clear();
		//validList.clear();
		trainMap.clear();
		testTripleMap.clear();
		testCountMap.clear();
		
		trainCount = 0;
		testCount = 0;
		//validCount = 0;
		
		rCount = new int[11];
		tCount = new int[11];
		rtCount = new int[11];
		hitCount = new int[11];
		trueCount = null;
		totalCount = null;
	}
	
	public static void WriteResult() throws IOException {
		System.out.println("==========result==========");
		WriteLog("==========result==========");
		
		for(int i = 1; i <= 10; i++) {
			for(int j = 0; j < graphCount; j++) {
				if(totalCount[j] > 0 && 1.0 * trueCount[j][i] / totalCount[j] < 1) {
					//System.out.println(j + " " + graphString.get(j) + " " + i + ": " + (1.0 * trueCount[j][i] / totalCount[j]));
					WriteLog(j + " " + graphString.get(j) + " " + i + ": " + (1.0 * trueCount[j][i] / totalCount[j]));
				}
			}
		}
		
		System.out.println("");
		WriteLog("");
		
		for(int i = 1; i <= 10; i++) {
			System.out.println("hitRate " + i + ": " + (1.0 * hitCount[i] / testCount));
			WriteLog("hitRate " + i + ": " + (1.0 * hitCount[i] / testCount));
		}
		
		for(int i = 1; i <= 10; i++) {
			System.out.println("precision " + i + ": " + (1.0 * rtCount[i] / rCount[i]));
			WriteLog("precision " + i + ": " + (1.0 * rtCount[i] / rCount[i]));
		}
		
		for(int i = 1; i <= 10; i++) {
			System.out.println("recall " + i + ": " + (1.0 * rtCount[i] / tCount[i]));
			WriteLog("recall " + i + ": " + (1.0 * rtCount[i] / tCount[i]));
		}
		
		for(int i = 1; i <= 10; i++) {
			double precision = 1.0 * rtCount[i] / rCount[i];
			double recall = 1.0 * rtCount[i] / tCount[i];
			System.out.println("f1 " + i + ": " + (2.0 * precision * recall / (precision + recall)));
			WriteLog("f1 " + i + ": " + (2.0 * precision * recall / (precision + recall)));
		}
	}
	
	public static void EvaluateK(int g, int depth, int edgeTail, String route, int graphId, HashMap<Integer, HashMap<String, ArrayList<NumWithIndex>>> map, HashMap<Integer, HashMap<Integer, Integer>> graph, ArrayList<NumWithIndex> result) throws IOException {
		HashMap<Integer, Integer> edge = graph.get(edgeTail);
		for (Map.Entry<Integer, Integer> entryEdge : edge.entrySet()) {
			int edgeHead = entryEdge.getKey();
			int edgeRelation = entryEdge.getValue();
			String trainMapKey = route;
			
			if(depth == 0) {
				trainMapKey += edgeRelation;
			}
			
			if(depth < g && ((depth > 0 && edgeRelation % 10 < 4) || depth == 0) && graph.containsKey(edgeHead)) {
				EvaluateK(g, depth + 1, edgeHead, edgeHead + "_" + trainMapKey, graphId, map, graph, result);
			}
		
			if(edgeRelation % 10 < 4 || depth == 0) {
				trainMapKey = edgeHead + "_" + trainMapKey;
				if(map.get(depth).containsKey(trainMapKey)) {
					ArrayList<NumWithIndex> value = map.get(depth).get(trainMapKey);
					for(int j = 0; j < value.size(); j++) {
						NumWithIndex addValue = value.get(j);
						
						int index = -1;
						for(int k = 0; k < result.size(); k++) {
							if(result.get(k).index == addValue.index) {
								index = k;
								break;
							}
						}
						
						int weight = 0;
						switch(depth) {
						case 0:
							weight = 1;
							break;
						case 1:
							weight = 10;
							break;
						case 2:
							weight = 100;
							break;
						case 3:
							weight = 1000;
							break;
						case 4:
							weight = 10000;
							break;
						case 5:
							weight = 100000;
							break;
						case 6:
							weight = 1000000;
							break;
						}
						
						if(index == -1) {
							NumWithIndex temp = new NumWithIndex(addValue.num * weight, addValue.index);
							result.add(temp);
						}
						else {
							result.get(index).num += addValue.num * weight;
						}
					}
				}
			}
			
			if(depth == 0) {
				String trainKey = edgeHead + "_" + edgeRelation;
				for(int j = 1; j <= 10; j++) {
					rCount[j] += j;
				}
				String countKey = edgeHead + "_" + edgeRelation + "_" + graphId;
				for(int j = 1; j <= 10; j++) {
					tCount[j] += testCountMap.get(countKey);
				}
				
				if(map.get(0).containsKey(trainKey)) {
					ComparatorNumWithIndexNum c = new ComparatorNumWithIndexNum();
					result.sort(c);
					
					for(int j = 0; j < 10; j++) {
						if(j >= result.size()) {
							break;
						}
						
						String tripleKey = edgeHead + "_" + result.get(j).index + "_" + edgeRelation + "_" + graphId;
						if(testTripleMap.containsKey(tripleKey)) {
							for(int k = j + 1; k <= 10; k++) {
								rtCount[k]++;
							}
						}
					}
					
					int pos = 0;
					int missCount = 0;
					String log = entityString.get(edgeHead) + "_" + entityString.get(edgeTail) + "_" + relationString.get(edgeRelation) + "_" + graphString.get(graphId) + " result:";
					while(pos < entityCount && missCount < 10) {
						if(pos >= result.size()) {
							break;
						}
						
						if(result.get(pos).index == edgeTail) {
							log += " " + entityString.get(result.get(pos).index);
							//System.out.println(log);
							WriteLog(log);
							for(int j = missCount + 1; j <= 10; j++) {
								hitCount[j]++;
								trueCount[graphId][j]++;
							}
							break;
						}
						
						String tripleKey = edgeHead + "_" + result.get(pos).index + "_" + edgeRelation + "_" + graphId;
						log += " " + entityString.get(result.get(pos).index);
						if(!testTripleMap.containsKey(tripleKey)) {
							missCount++;
						}
						pos++;
					}
				}
			}
		}
	}
	
	public static void Evaluate(int g, HashMap<Integer, HashMap<String, ArrayList<NumWithIndex>>> map) throws IOException {
		System.out.println("==========evaluate==========");
		WriteLog("==========evaluate==========");
		
		int icount = 0;
		int lastGraph = -1;
		HashMap<Integer, HashMap<Integer, Integer>> graph = new HashMap<Integer, HashMap<Integer, Integer>>();
		for (Map.Entry<Integer, HashMap<Integer, Integer>> entry : graph.entrySet()) {
			entry.getValue().clear();
		}
		graph.clear();
		
		for(int i = 0; i < testCount; i++) {
			icount++;
			if(icount % 1000 == 0){
				System.out.println("count: " + (icount));
				WriteLog("count: " + (icount));
			}
			
			int[] triple = testList.get(i);
			
			totalCount[triple[3]]++;
			
			if(triple[3] != lastGraph) {
				if(lastGraph == -1) {
					lastGraph = triple[3];
				}
				else {
					for (Map.Entry<Integer, HashMap<Integer, Integer>> entryGraph : graph.entrySet()) {
						int edgeTail = entryGraph.getKey();
						ArrayList<NumWithIndex> result = new ArrayList<NumWithIndex>();
						EvaluateK(g, 0, edgeTail, "", lastGraph, map, graph, result);
					}
					
					for (Map.Entry<Integer, HashMap<Integer, Integer>> entry : graph.entrySet()) {
						entry.getValue().clear();
					}
					graph.clear();
					lastGraph = triple[3];
				}
			}

			if(graph.containsKey(triple[1])) {
				HashMap<Integer, Integer> value = graph.get(triple[1]);
				value.put(triple[0], triple[2]);
			}
			else {
				HashMap<Integer, Integer> value = new HashMap<Integer, Integer>();
				value.put(triple[0], triple[2]);
				graph.put(triple[1], value);
			}
		}
		
		for (Map.Entry<Integer, HashMap<Integer, Integer>> entryGraph : graph.entrySet()) {
			int edgeTail = entryGraph.getKey();
			ArrayList<NumWithIndex> result = new ArrayList<NumWithIndex>();
			EvaluateK(g, 0, edgeTail, "", lastGraph, map, graph, result);
		}
	}
	
	public static int ReadId(String filePath, HashMap<String, Integer> map, HashMap<Integer, String> mapString) throws IOException {
		int result = 0;
		
		System.out.println("==========file: " + filePath + "==========");
		WriteLog("==========file: " + filePath + "==========");
		File file = new File(filePath);
		
		DataInputStream input = new DataInputStream(new FileInputStream(file));
		CSVReader reader = new CSVReader(new InputStreamReader(input, encodingText), separator); //用csvReader处理文件
		List<String[]> myEntries;
		myEntries = reader.readAll();
		String[][] rowData = myEntries.toArray(new String[0][]);
		reader.close();
		
		result = rowData.length;
		
		int icount = 0;
		for (int i = rowData.length - 1; i >= 0; i--)
		{
			icount++;
			if(icount % 10000 == 0){
				System.out.println("count: " + (icount));
				WriteLog("count: " + (icount));
			}
			
			map.put(rowData[i][0], Integer.parseInt(rowData[i][1]));
			mapString.put(Integer.parseInt(rowData[i][1]), rowData[i][0]);
		}
		
		return result;
	}
	
	public static void ReadTrainK(int g, int depth, int edgeHead, String route, HashMap<Integer, HashMap<String, ArrayList<NumWithIndex>>> map, HashMap<Integer, HashMap<Integer, Integer>> graph) {
		HashMap<Integer, Integer> edge = graph.get(edgeHead);
		for (Map.Entry<Integer, Integer> entryEdge : edge.entrySet()) {
			int edgeTail = entryEdge.getKey();
			int edgeRelation = entryEdge.getValue();
			
			if(depth < g && edgeRelation % 10 < 4 && graph.containsKey(edgeTail)) {
				ReadTrainK(g, depth + 1, edgeTail, route + edgeTail + "_", map, graph);
			}
			
			String trainKey = route + edgeRelation;
			if(map.get(depth).containsKey(trainKey)) {
				ArrayList<NumWithIndex> value = map.get(depth).get(trainKey);
				int index = -1;
				for(int j = 0; j < value.size(); j++) {
					if(value.get(j).index == edgeTail) {
						index = j;
						break;
					}
				}
				if(index == -1) {
					NumWithIndex temp = new NumWithIndex(1, edgeTail);
					value.add(temp);
				}
				else {
					value.get(index).num++;
				}
			}
			else {
				ArrayList<NumWithIndex> value = new ArrayList<NumWithIndex>();
				NumWithIndex temp = new NumWithIndex(1, edgeTail);
				value.add(temp);
				map.get(depth).put(trainKey, value);
			}
		}
	}
	
	public static int ReadTrain(int g, String filePath, HashMap<Integer, HashMap<String, ArrayList<NumWithIndex>>> map) throws IOException {
		int result = 0;
		
		System.out.println("==========file: " + filePath + "==========");
		WriteLog("==========file: " + filePath + "==========");
		File file = new File(filePath);
		
		DataInputStream input = new DataInputStream(new FileInputStream(file));
		CSVReader reader = new CSVReader(new InputStreamReader(input, encodingText), separator);
		List<String[]> myEntries;
		myEntries = reader.readAll();
		String[][] rowData = myEntries.toArray(new String[0][]);
		reader.close();
		
		result = rowData.length;
		
		int icount = 0;
		int lastGraph = -1;
		HashMap<Integer, HashMap<Integer, Integer>> graph = new HashMap<Integer, HashMap<Integer, Integer>>();
		for (Map.Entry<Integer, HashMap<Integer, Integer>> entry : graph.entrySet()) {
			entry.getValue().clear();
		}
		graph.clear();
		
		for (int i = rowData.length - 1; i >= 0; i--)
		{
			icount++;
			if(icount % 1000 == 0){
				System.out.println("count: " + (icount));
				WriteLog("count: " + (icount));
			}
			
			int[] triple = new int[4];
			triple[0] = entityId.get(rowData[i][0]);
			triple[1] = entityId.get(rowData[i][1]);
			triple[2] = relationId.get(rowData[i][2]);
			triple[3] = graphId.get(rowData[i][3]);
			//list.add(triple);
			
			if(triple[3] != lastGraph) {
				if(lastGraph == -1) {
					lastGraph = triple[3];
				}
				else {
					for (Map.Entry<Integer, HashMap<Integer, Integer>> entryGraph : graph.entrySet()) {
						int edgeHead = entryGraph.getKey();
						ReadTrainK(g, 0, edgeHead, edgeHead + "_", map, graph);
					}
					
					for (Map.Entry<Integer, HashMap<Integer, Integer>> entry : graph.entrySet()) {
						entry.getValue().clear();
					}
					graph.clear();
					lastGraph = triple[3];
				}
			}
			
			if(graph.containsKey(triple[0])) {
				HashMap<Integer, Integer> value = graph.get(triple[0]);
				value.put(triple[1], triple[2]);
			}
			else {
				HashMap<Integer, Integer> value = new HashMap<Integer, Integer>();
				value.put(triple[1], triple[2]);
				graph.put(triple[0], value);
			}
		}
		
		for (Map.Entry<Integer, HashMap<Integer, Integer>> entryGraph : graph.entrySet()) {
			int edgeHead = entryGraph.getKey();
			ReadTrainK(g, 0, edgeHead, edgeHead + "_", map, graph);
		}
		
		return result;
	}
	
	public static int ReadTest(String filePath, ArrayList<int[]> list, HashMap<String, Boolean> tripleMap, HashMap<String, Integer> countMap) throws IOException {
		int result = 0;
		
		System.out.println("==========file: " + filePath + "==========");
		WriteLog("==========file: " + filePath + "==========");
		File file = new File(filePath);
		
		DataInputStream input = new DataInputStream(new FileInputStream(file));
		CSVReader reader = new CSVReader(new InputStreamReader(input, encodingText), separator);
		List<String[]> myEntries;
		myEntries = reader.readAll();
		String[][] rowData = myEntries.toArray(new String[0][]);
		reader.close();
		
		result = rowData.length;
		
		int icount = 0;
		for (int i = rowData.length - 1; i >= 0; i--)
		{
			icount++;
			if(icount % 1000 == 0){
				System.out.println("count: " + (icount));
				WriteLog("count: " + (icount));
			}
			
			int[] triple = new int[4];
			triple[0] = entityId.get(rowData[i][0]);
			triple[1] = entityId.get(rowData[i][1]);
			triple[2] = relationId.get(rowData[i][2]);
			triple[3] = graphId.get(rowData[i][3]);
			list.add(triple);
			
			String tripleKey = triple[0] + "_" +  triple[1] + "_" +  triple[2] + "_" +  triple[3];
			if(!tripleMap.containsKey(tripleKey)) {
				tripleMap.put(tripleKey, true);
			}
			
			String countKey = triple[0] + "_" +  triple[2] + "_" +  triple[3];
			if(countMap.containsKey(countKey)) {
				countMap.replace(countKey, countMap.get(countKey) + 1);
			}
			else {
				countMap.put(countKey, 1);
			}
		}
		
		return result;
	}
	
	public static void swap(int[] array, int i, int j) {
		array[i] = array[i] ^ array[j];
		array[j] = array[i] ^ array[j];
		array[i] = array[i] ^ array[j];
	}
	
    public static void WriteLog(String strings) throws IOException {  
        bw.write(strings + "\r\n");
    }
}
