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

public class ScorePMML_ElNinoTest {

	// ElNino Models
	private String regressionElNinoModelPath = "./resources/examples/models/elnino_linearReg.xml";

    private TupleFactory tf = TupleFactory.getInstance();

	// --------------------------
	// ElNino Test Functions
	// --------------------------

	@Test
	public void regressionScoringTest_ElNino_1() throws IOException, SAXException, JAXBException {

        Schema inputSchema = buildElNinoInputSchema();
        
        // Input/Output Bag
        Tuple inputTuple = tf.newTuple();
        Tuple expected   = tf.newTuple();
        {
        	// Visit 1, Input: Implicit Signout
        	inputTuple = this.buildElNinoInputEvent("1","1","1","8.96","-140.32","-6.3","-6.4","83.5","27.32","27.57");
            
            // Visit 1, Output
        	expected = this.buildElNinoOutputEvent("1","1","1","8.96","-140.32","-6.3","-6.4","83.5","27.32","27.57",27.384241597858438);
        }

        // Initialize Class
        ScorePMML evalPMML = new ScorePMML(this.regressionElNinoModelPath);
        Schema outputSchema = evalPMML.outputSchema(inputSchema);
        Tuple observed = evalPMML.exec(inputTuple);

        // Test
        if (expected.equals(observed)) {
        	System.out.println("regressionScoringTest_ElNino_1: PASS");
        } else {
        	System.out.println("---------- EPIC FAIL: regressionScoringTest_ElNino_1 ----------");
        	System.out.println("Expected: " + expected.toString());
        	System.out.println("Observed: " + observed.toString());
        	System.out.println("-------- END EPIC FAIL --------");
        }
        
        assertEquals(expected,observed);
	}

	// --------------------------
	// El Nino Helper Functions
	// --------------------------

    private Schema buildElNinoInputSchema() throws FrontendException {

    	// Build Field Schema
    	List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        fieldSchemas.add(new Schema.FieldSchema("buoy_day_ID", DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("buoy"       , DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("day"        , DataType.CHARARRAY));
        fieldSchemas.add(new Schema.FieldSchema("latitude"   , DataType.DOUBLE   ));
        fieldSchemas.add(new Schema.FieldSchema("longitude"  , DataType.DOUBLE   ));
        fieldSchemas.add(new Schema.FieldSchema("zon_winds"  , DataType.DOUBLE   ));
        fieldSchemas.add(new Schema.FieldSchema("mer_winds"  , DataType.DOUBLE   ));
        fieldSchemas.add(new Schema.FieldSchema("humidity"   , DataType.DOUBLE   ));
        fieldSchemas.add(new Schema.FieldSchema("airtemp"    , DataType.DOUBLE   ));
        fieldSchemas.add(new Schema.FieldSchema("s_s_temp"   , DataType.DOUBLE   ));

        return new Schema(fieldSchemas);

    }

    private Tuple buildElNinoInputEvent( String buoy_day_ID, String buoy, String day, String latitude, String longitude, String zon_winds, String mer_winds, String humidity, String airtemp, String s_s_temp) {

        Tuple newTuple = tf.newTuple();
        newTuple.append(buoy_day_ID);
        newTuple.append(buoy       );
        newTuple.append(day        );
        newTuple.append(latitude   );
        newTuple.append(longitude  );
        newTuple.append(zon_winds  );
        newTuple.append(mer_winds  );
        newTuple.append(humidity   );
        newTuple.append(airtemp    );
        newTuple.append(s_s_temp   );

        return newTuple;
    }

    private Tuple buildElNinoOutputEvent( String buoy_day_ID, String buoy, String day, String latitude, String longitude, String zon_winds, String mer_winds, String humidity, String airtemp, String s_s_temp, double airtemp_predicted) {

		Tuple newTuple = tf.newTuple();
		newTuple.append(airtemp_predicted);
		
		return newTuple;
    }

}
