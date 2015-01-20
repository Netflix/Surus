package com.netflix.pmml.open;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.JAXBException;

import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.*;
import org.xml.sax.SAXException;

import com.netflix.pmml.ScorePMML;

public class ScorePMML_IrisTest {

	// Iris Models
	private String treeIrisModelPath = "./resources/examples/models/single_iris_dectree.xml";
	private String nnIrisModelPath   = "./resources/examples/models/single_iris_mlp.xml";
	private String rfIrisModelPath   = "./resources/examples/models/example.randomForest.xml";
	
    private TupleFactory tf = TupleFactory.getInstance();
	
	// --------------------------
	// Iris Test Functions
	// --------------------------

	@Test
	public void treeScoringTest_Iris_1() throws IOException, SAXException, JAXBException {

		// Build Input Schema
        Schema inputSchema = buildIrisInputSchema();
        
        // Input/Output Bag
        Tuple inputTuple = tf.newTuple();
        Tuple expected   = tf.newTuple();
        {
        	// Visit 1, Input: Implicit Signout
        	inputTuple = this.buildIrisInputEvent(5.1,3.5,1.4,0.2,"Iris-setosa");
            
            // Visit 1, Output
            expected = this.buildIrisOutputEvent("Iris-setosa");
        }

        // Initialize Class
        ScorePMML evalPMML = new ScorePMML(this.treeIrisModelPath);
        Schema outputScheam = evalPMML.outputSchema(inputSchema);
        Tuple observed = evalPMML.exec(inputTuple);

        // Test
        if (expected.equals(observed)) {
        	System.out.println("treeScoringTest_Iris_1: PASS");
        } else {
        	System.out.println("---------- EPIC FAIL: treeScoringTest_Iris_1 ----------");
        	System.out.println("Expected: " + expected.toString());
        	System.out.println("Observed: " + observed.toString());
        	System.out.println("-------- END EPIC FAIL --------");
        }
        
        assertEquals(expected,observed);
	}

	@Test
	public void treeScoringTest_Iris_2() throws IOException, SAXException, JAXBException {

		// Build Input Schema
        Schema inputSchema = buildIrisInputSchema();
        
        // Input/Output Bag
        Tuple inputTuple = tf.newTuple();
        Tuple expected   = tf.newTuple();
        {
        	// Visit 1, Input: Implicit Signout
        	inputTuple = this.buildIrisInputEvent(5.9,3.2,4.8,1.8,"Iris-versicolor");
            
            // Visit 1, Output
            expected = this.buildIrisOutputEvent("Iris-virginica");
        }

        // Initialize Class
        ScorePMML evalPMML = new ScorePMML(this.treeIrisModelPath);
        Schema outputScheam = evalPMML.outputSchema(inputSchema);
        Tuple observed = evalPMML.exec(inputTuple);

        // Test
        if (expected.equals(observed)) {
        	System.out.println("treeScoringTest_Iris_2: PASS");
        } else {
        	System.out.println("---------- EPIC FAIL: treeScoringTest_Iris_2 ----------");
        	System.out.println("Expected: " + expected.toString());
        	System.out.println("Observed: " + observed.toString());
        	System.out.println("-------- END EPIC FAIL --------");
        }
        
        assertEquals(expected,observed);
	}

	@Test
	public void nnScoringTest_Iris_1() throws IOException, SAXException, JAXBException {

		// Build Input Schema
        Schema inputSchema = buildIrisInputSchema();
        
        // Input/Output Bag
        Tuple inputTuple = tf.newTuple();
        Tuple expected   = tf.newTuple();
        {
        	// Visit 1, Input: Implicit Signout
        	inputTuple = this.buildIrisInputEvent(5.9,3.2,4.8,1.8,"Iris-versicolor");
            
            // Visit 1, Output
            expected = this.buildIrisOutputEvent("Iris-versicolor");
        }

        // Initialize Class
        ScorePMML evalPMML = new ScorePMML(this.nnIrisModelPath);
        Schema outputScheam = evalPMML.outputSchema(inputSchema);
        Tuple observed = evalPMML.exec(inputTuple);

        // Test
        if (expected.equals(observed)) {
        	System.out.println("nnScoringTest_Iris_1: PASS");
        } else {
        	System.out.println("---------- EPIC FAIL: nnScoringTest_Iris_1 ----------");
        	System.out.println("Expected: " + expected.toString());
        	System.out.println("Observed: " + observed.toString());
        	System.out.println("-------- END EPIC FAIL --------");
        }
        
        
        assertEquals(expected,observed);
	}

	
	@Test
	public void rfScoringTest_Iris_1() throws IOException, SAXException, JAXBException {

		// Build Input Schema
        Schema inputSchema = buildIrisInputSchema();
        
        // Input/Output Bag
        Tuple inputTuple = tf.newTuple();
        Tuple expected   = tf.newTuple();
        {
        	// Visit 1, Input: Implicit Signout
        	inputTuple = this.buildIrisInputEvent(5.1,3.5,1.4,0.2,"setosa");
            
            // Visit 1, Output
            expected = this.buildIrisOutputEvent("setosa","setosa",1.0,0.0,0.0);
        }

        // Initialize Class
        ScorePMML evalPMML = new ScorePMML(this.rfIrisModelPath);
        Schema outputSchema = evalPMML.outputSchema(inputSchema);
        Tuple observed = evalPMML.exec(inputTuple);

        // Test
        if (expected.equals(observed)) {
        	System.out.println("rfScoringTest_Iris_1: PASS");
        } else {
        	System.out.println("---------- EPIC FAIL: rfScoringTest_Iris_1 ----------");
        	System.out.println("Expected: " + expected.toString());
        	System.out.println("Observed: " + observed.toString());
        	System.out.println("-------- END EPIC FAIL --------");
        }
        
        assertEquals(expected,observed);
	}



	// --------------------------
	// Iris Helper Functions
	// --------------------------

    private Schema buildIrisInputSchema() throws FrontendException {

    	// Build Field Schema
    	List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        fieldSchemas.add(new Schema.FieldSchema("sepal_length"  , DataType.DOUBLE));
        fieldSchemas.add(new Schema.FieldSchema("sepal_width"   , DataType.DOUBLE));
        fieldSchemas.add(new Schema.FieldSchema("petal_length"  , DataType.DOUBLE));
        fieldSchemas.add(new Schema.FieldSchema("petal_width"	, DataType.DOUBLE));
        fieldSchemas.add(new Schema.FieldSchema("species"		, DataType.CHARARRAY));

        return new Schema(fieldSchemas);
    }

    private Tuple buildIrisInputEvent(double sepal_length, double sepal_width, double petal_length, double petal_width, String inputClass) {

        Tuple newTuple = tf.newTuple();
        newTuple.append(sepal_length);
        newTuple.append(sepal_width);
        newTuple.append(petal_length);
        newTuple.append(petal_width);
        newTuple.append(inputClass);

        return newTuple;
    }

    private Tuple buildIrisOutputEvent(String predictedClass) {

        Tuple newTuple = tf.newTuple();
        newTuple.append(predictedClass);

        return newTuple;
    }

    private Tuple buildIrisOutputEvent(String predictedClass, String outputField_Class, double predictedClass1, double predictedClass2, double predictedClass3) {

        Tuple newTuple = tf.newTuple();
        newTuple.append(predictedClass);
        newTuple.append(outputField_Class);
        newTuple.append(predictedClass1);
        newTuple.append(predictedClass2);
        newTuple.append(predictedClass3);

        return newTuple;
    }

}
