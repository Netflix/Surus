package com.netflix.pmml;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.apache.pig.impl.util.UDFContext;
import org.dmg.pmml.DataField;
import org.dmg.pmml.FieldName;
import org.dmg.pmml.IOUtil;
import org.dmg.pmml.OutputField;
import org.dmg.pmml.PMML;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorUtil;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.ModelEvaluatorFactory;
import org.jpmml.manager.ModelManager;
import org.jpmml.manager.PMMLManager;
import org.xml.sax.SAXException;

public class ScorePMML extends EvalFunc<Tuple> {

	private Evaluator 		evaluator		= null;
	private List<FieldName> activeFields	= null;
	private List<FieldName> predictedFields	= null;
	private List<FieldName> outputFields	= null;
	private String 			modelPath		= null;
	private String 			modelName		= null;
	private Schema 			inputTupleSchema = null;
	private Map<String,Integer> aliasMap 	 = null;
	private Boolean 		failOnTypeMatching = true;
	Map<FieldName, FieldValue> preparedRow = new LinkedHashMap<FieldName, FieldValue>();

	
    private static final TupleFactory tf = TupleFactory.getInstance();

	private static final Map<String, Byte> dataTypeMap = new HashMap<String, Byte>();
    static {
        dataTypeMap.put("STRING" , DataType.CHARARRAY);
        dataTypeMap.put("INTEGER", DataType.INTEGER);
        dataTypeMap.put("FLOAT"  , DataType.DOUBLE);
        dataTypeMap.put("LONG"   , DataType.DOUBLE);
        dataTypeMap.put("DOUBLE" , DataType.DOUBLE);
        dataTypeMap.put("BOOLEAN", DataType.DOUBLE);
    }

	// Constructor
	public ScorePMML(String... params) throws IOException, SAXException, JAXBException {

		// Call Nested Constructor
		this(params[0]);

		// Override default failure mode
		if (params.length == 2) {
			this.failOnTypeMatching = Boolean.parseBoolean(params[1]);
		}

	}	
	
	// Constructor
	public ScorePMML(String modelPath) throws IOException, SAXException, JAXBException {

		// Set Default failure mode
		this.failOnTypeMatching = true;
		
		// Set Model Path
		this.modelPath = modelPath;
		System.err.println("modelPath: "+this.modelPath);

		// Set Distributed Cache
    	int blah = this.modelPath.lastIndexOf("/") + 1;
    	this.modelName = this.modelPath.substring(blah);
       	System.err.println("modelName: "+this.modelName);

	}
	
    public List<String> getCacheFiles() {
    	String filePath = this.modelPath+"#"+this.modelName;
        List<String> list = new ArrayList<String>(1); 
        list.add(filePath);
        System.err.println(filePath+": added to the distributed cache.");
        return list; 
    } 
	
	private void initialize(Schema inputSchema) throws IOException, SAXException, JAXBException {

		this.inputTupleSchema = inputSchema;

		// and, initialize aliasMap:
		if (this.aliasMap == null) {
			this.aliasMap = new HashMap<String,Integer>();
			for (String alias : this.inputTupleSchema.getAliases()) {
				this.aliasMap.put(alias,this.inputTupleSchema.getPosition(alias));		// something to cleanup
			}
		}

		// Get PMML Object
		PMML pmml = null;
		try {
			
			/*
			 * TODO: Make this more robust. Specifically, Angela Ho wanted to refernce a file in the distributed
			 * 		 cache directly.  Obviously, my code doesn't support this, because it would try to open
			 * 	     the file with the IOUtil Java object, as opposed to the hadoop.fs.Path object.
			 * 
			 * TODO: This try/catch block is a hack for:
			 * 		(1) checking if execution is being done on "back-end."  A check for back-end can be done with 
			 * 			UDFContext.getUDFContext().isFrontend() BUT this does not resolve problems with local-mode.
			 * 		(2) enables testing in local-mode without failing unit tests.
			 */
			
			// Try reading file from distributed cache.
    		pmml = IOUtil.unmarshal(new File("./"+this.modelName));
    		System.err.println("Read model from distributed cache!");
    		
		} catch (Throwable t) {
			// If not on the back-end... (and distributed cache not available) ...
			
			if (this.modelPath.toLowerCase().startsWith("s3n://") || this.modelPath.toLowerCase().startsWith("s3://")) {
				// ... read from S3.
				Path path = new Path(this.modelPath);
				FileSystem fs = path.getFileSystem(new Configuration());
				FSDataInputStream in = fs.open(path);
				pmml = IOUtil.unmarshal(in);
	    		System.err.println("Read model from s3!");

			} else {
				// ... read from local file.
				pmml = IOUtil.unmarshal(new File(this.modelPath));
	    		System.err.println("Read model from local disk!");
			}

		}

		// Initialize the pmmlManager
		PMMLManager pmmlManager = new PMMLManager(pmml);
		
		// Initialize the PMML Model Manager
		ModelManager<?> modelManager = pmmlManager.getModelManager(null, ModelEvaluatorFactory.getInstance());

		this.evaluator 		 = (Evaluator)modelManager;			// Model Evaluator
		this.activeFields 	 = evaluator.getActiveFields();		// input columns
		this.predictedFields = evaluator.getPredictedFields();	// predicted columns
		this.outputFields 	 = evaluator.getOutputFields();		// derived output columns (based on predicted columns)

	}
	
	// Define Output Schema
    @Override
    public Schema outputSchema(Schema input) {

		try {
			initialize(input);
		} catch (Throwable t) {
			throw new RuntimeException("Frontend: Unable to initialize PMML file: ",t);
		}

    	// Define the output schema:
        try {

        	// Define Input Tuple Schema
            this.inputTupleSchema = input;
            HashSet<String> aliases = new HashSet<String>(inputTupleSchema.getAliases());
            Boolean isVerbose = false;

            for (FieldName activeField : this.activeFields) {
            	
            	// Check that all active fields are present in dataset:
            	String activeFieldAlias = activeField.toString().toLowerCase();
            	if (!aliases.contains(activeFieldAlias)) {
                    throw new RuntimeException("ERROR: "+activeFieldAlias+" is not in the input dataset!");
            	}
            	
            	// Check that all active fields have expected datatypes:
    			Byte left = this.inputTupleSchema.getField(aliasMap.get(activeFieldAlias)).type;
    			Byte right = dataTypeMap.get(this.evaluator.getDataField(activeField).getDataType().toString());
            	if (left != right)
            		if (failOnTypeMatching) {
                        throw new RuntimeException("ERROR: "+activeFieldAlias+" does not match expected type! (Expected: "
                        		+DataType.genTypeToNameMap().get(right)+" Observed: "+DataType.genTypeToNameMap().get(left)+")");
            		} else if (UDFContext.getUDFContext().isFrontend() && !isVerbose) {
            			System.err.println("WARNING: active fields do not match expected type! Please run in strict mode to determine which fields are in violation");
            			isVerbose = true;
            			// System.err.println("WARNING: "+activeFieldAlias+" does not match expected type! (Expected: "
                        // 		+DataType.genTypeToNameMap().get(right)+" Observed: "+DataType.genTypeToNameMap().get(left)+")");
            		}
            }
            
        	// Create List of Tuple Values
        	List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        	
        	// Predicted Fields
        	for (FieldName predictedField : this.predictedFields) {
        		String predictedFieldAlias = "predictedField_" + predictedField.toString().toLowerCase();

        		// Create FieldName
    			DataField dataField = this.evaluator.getDataField(predictedField);
    			String dataType = dataField.getDataType().toString();
    			
    			if (dataType == null) {
                    throw new RuntimeException("Predicted Fields with unknown datatype are not supported! Column: "+predictedFieldAlias+", PMML DataType "+dataType+".");
    			} else if (!dataTypeMap.containsKey(dataType)) {
                    throw new RuntimeException("Column: "+predictedFieldAlias+", PMML DataType "+dataType+" is not currently supported.");
    			} else {
            		fieldSchemas.add(new Schema.FieldSchema(predictedFieldAlias,dataTypeMap.get(dataType)));
    			}
        	}

        	// Output Fields
        	for (FieldName outputField : this.outputFields) {
        		String outputFieldAlias = "outputField_" + outputField.toString().toLowerCase();
        		
        		// Create FieldName
    			OutputField dataField = this.evaluator.getOutputField(outputField);
    			if (dataField.getDataType() == null) {
            		fieldSchemas.add(new Schema.FieldSchema(outputFieldAlias,DataType.BYTEARRAY));
    			} else if (dataTypeMap.containsKey(dataField.getDataType().toString())) {
            		fieldSchemas.add(new Schema.FieldSchema(outputFieldAlias,dataTypeMap.get(dataField.getDataType().toString())));
    			} else {
                    throw new RuntimeException("Column: "+outputFieldAlias+", PMML DataType "+dataField.getDataType().toString()+" is not currently supported.");
    			}
        	}

            // Build Tuple and Wrap in DataBag
            FieldSchema tupleFieldSchema = new FieldSchema("EvalPMML", new Schema(fieldSchemas), DataType.TUPLE);
            
            // Return Schema
            Schema outputSchema = new Schema(tupleFieldSchema);
            return outputSchema;
            
        } catch (Throwable t) {
        	System.err.println(t);
            throw new RuntimeException(t);
        }

    }
	
    // Define Exec
	@Override
	public Tuple exec(Tuple input) throws IOException {

		// check
		int dummy = 0;
		
		// Initialize Evaluator if null:
		if (this.evaluator == null) {
			try {
				System.out.println("Initializing: "+(dummy++)+" time");
				Schema inputSchema = getInputSchema();
				this.initialize(inputSchema);			// something to check
			} catch (Throwable t) {
				throw new RuntimeException("Backend: Unable to initialize PMML file: ",t);
			}
		}

		// Initialize Output as Input
		Tuple outputTuple = tf.newTuple(this.predictedFields.size() + this.outputFields.size());

		/* ************************
		// BLOCK: Prepare Data
		************************* */
		
		for(FieldName inputField : this.activeFields){

			// Get Object
			Object origBodyCell = (Object) input.get(aliasMap.get(inputField.getValue().toLowerCase()));
			
			Object bodyCell;
			if (origBodyCell instanceof Long) {
				bodyCell = ((Long) origBodyCell).doubleValue();
			} else {
				bodyCell = origBodyCell;
			}

			// Prepare Object for Scoring
			this.preparedRow.put(inputField, EvaluatorUtil.prepare(this.evaluator, inputField, bodyCell));

			// Prepare Object for Scoring
			// CC: Removed this b/c I think the "Long" check above resolves any issues.
			/*
			try {
				this.preparedRow.put(inputField, EvaluatorUtil.prepare(this.evaluator, inputField, bodyCell));
			} catch (Throwable t) {
	        	System.err.println("Unable to prepare record, Trouble Parsing: " + inputField.toString() + " (value="+ bodyCell+")");
	        	System.err.println(t);
	            throw new RuntimeException(t);
			}
			*/

		}

		// Score Data
		Map<FieldName, ?> result = evaluator.evaluate(this.preparedRow);

		// Append Predicted Fields
		int i = 0;
		for(FieldName predictedField : this.predictedFields){
			outputTuple.set(i++,EvaluatorUtil.decode(result.get(predictedField)));
		}

		for(FieldName outputField : this.outputFields){
			outputTuple.set(i++,EvaluatorUtil.decode(result.get(outputField)));
		}

		// Return Tuple:
		return outputTuple;

	}
	
}
