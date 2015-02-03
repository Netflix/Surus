package org.surus.pig;

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
import org.surus.pig.ScorePMML;
import org.xml.sax.SAXException;

public class ScorePMML_AuditTest {

	// Audit Models
	private String ensembleAuditModelPath = "./resources/examples/models/ensemble_audit_dectree.xml";

	// Tuple Factory
    private TupleFactory tf = TupleFactory.getInstance();

	// --------------------------
	// Audit Test Functions
	// --------------------------

	@Test
	public void ensembleScoringTest_Audit_1() throws IOException, SAXException, JAXBException {

        Schema inputSchema = buildAuditInputSchema();
        
        // Input/Output Bag
        Tuple inputTuple = tf.newTuple();
        Tuple expected   = tf.newTuple();
        {
        	// Visit 1, Input: Implicit Signout
        	inputTuple = this.buildAuditInputEvent(1038288L,45,"Private","Bachelor","Married","Repair",27743.82,"Male",0,55,"UnitedStates",7298,1);
            
            // Visit 1, Output
        	expected = this.buildAuditOutputEvent(1038288L,45,"Private","Bachelor","Married","Repair",27743.82,"Male",0,55,"UnitedStates",7298,1,"0");
        }

        // Initialize Class
        ScorePMML evalPMML = new ScorePMML(this.ensembleAuditModelPath);
        Schema outputScheam = evalPMML.outputSchema(inputSchema);
        Tuple observed = evalPMML.exec(inputTuple);

        // Test
        if (expected.equals(observed)) {
        	System.out.println("ensembleScoringTest_Audit_1: PASS");
        } else {
        	System.out.println("---------- EPIC FAIL: ensembleScoringTest_Audit_1 ----------");
        	System.out.println("Expected: " + expected.toString());
        	System.out.println("Observed: " + observed.toString());
        	System.out.println("-------- END EPIC FAIL --------");
        }
        
        assertEquals(expected,observed);
	}

	// --------------------------
	// Audit Helper Functions
	// --------------------------

    private Schema buildAuditInputSchema() throws FrontendException {

    	// Build Field Schema
    	List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        fieldSchemas.add(new Schema.FieldSchema("id"             , DataType.LONG));
        fieldSchemas.add(new Schema.FieldSchema("age"            , DataType.INTEGER));
        fieldSchemas.add(new Schema.FieldSchema("employment"     , DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("education"      , DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("marital"        , DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("occupation"     , DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("income"         , DataType.DOUBLE));
        fieldSchemas.add(new Schema.FieldSchema("gender"         , DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("deductions"     , DataType.DOUBLE));
        fieldSchemas.add(new Schema.FieldSchema("hours"          , DataType.INTEGER));
        fieldSchemas.add(new Schema.FieldSchema("ignore_accounts", DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("risk_adjustment", DataType.INTEGER));
        fieldSchemas.add(new Schema.FieldSchema("target_adjusted", DataType.INTEGER));

        return new Schema(fieldSchemas);

    }

    private Tuple buildAuditInputEvent( Long    ID             
    	                              , Integer Age            
    	                              , String  Employment     
    	                              , String  Education      
    	                              , String  Marital        
    	                              , String  Occupation     
    	                              , Double  Income         
    	                              , String  Gender         
    	                              , Integer Deductions     
    	                              , Integer Hours          
    	                              , String  IGNORE_Accounts
    	                              , Integer RISK_Adjustment
    	                              , Integer TARGET_Adjusted) {
    	
    	
        Tuple newTuple = tf.newTuple();
        newTuple.append(ID             );
        newTuple.append(Age            );
        newTuple.append(Employment     );
        newTuple.append(Education      );
        newTuple.append(Marital        );
        newTuple.append(Occupation     );
        newTuple.append(Income         );
        newTuple.append(Gender         );
        newTuple.append(Deductions     );
        newTuple.append(Hours          );
        newTuple.append(IGNORE_Accounts);
        newTuple.append(RISK_Adjustment);
        newTuple.append(TARGET_Adjusted);

        return newTuple;
    }

    private Tuple buildAuditOutputEvent( Long    ID             
                                       , Integer Age            
                                       , String  Employment     
                                       , String  Education      
                                       , String  Marital        
                                       , String  Occupation     
                                       , Double  Income         
                                       , String  Gender         
                                       , Integer Deductions     
                                       , Integer Hours          
                                       , String  IGNORE_Accounts
                                       , Integer RISK_Adjustment
                                       , Integer TARGET_Adjusted
                                       , String  TARGET_Adjusted_predicted) {

		Tuple newTuple = tf.newTuple();
		newTuple.append(TARGET_Adjusted_predicted);
		
		return newTuple;
    }

}
