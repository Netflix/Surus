package org.surus.pig;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import org.surus.pig.RAD;


public class RAD_Test {

    private TupleFactory tf = TupleFactory.getInstance();
    private BagFactory   bf = BagFactory.getInstance();

    private static final String[] argsDaily8  = new String[]{"metric","8","7"};
    private static final String[] argsDaily9  = new String[]{"metric","9","7","False"};

    
    @Test
    public void testNormal() throws Exception {
        System.out.println("testNormal");
        
        double[] ts  = new double[] {2.05407309078346,2.85886923211884,2.89728554463089,0.790480493540229,0.548595335194215,1.31367506547418,1.74407133897301,4.06071962679526,2.75651081738515,0.604658754735038,0.182607837501951,-1.262201503678,0.996560864201235,2.74637817075616,0.775004762296101,0.906823901472144,2.6839457174704,-0.0625841462071901,-1.09641353766956,0.00479165991036998,0.449351175604642,3.53152043857777,1.05206417605014,2.7864942275709,-0.691007430091048,-1.02038488026721,-1.35124486835257,0.0621976297222073,2.82421545538541,2.41312411015615,1.27711183784622,0.0988204592711682,1.50691474460298,0.272037685359444,1.9889742629239,3.33907184622517,3.68134545243902,0.751559686193563,0.679120355399832,0.428056866405207,0.351341204822829,1.33498418531095,3.04169869243666,1.22542459625713,1.35457091793328,0.567124649501233,-1.95560538335988,-1.09014280752067,1.80062291606412,0.588637569785287,1.89212604693897,1.38386740607786,0.356716316822486,-2.07161693692556,4,1.44451323393473,3.52551739267569,3.16481926426412,1.83839333727511,0.827646664705546,0.654351159135431,-0.00892931340717523,0.678082675364184};
        double[] E_r = new double[] {0.3318797478729918,1.373638963651734,1.5863429313355741,-0.13690908975775629,-0.17341746498876717,0.45656608096044515,0.5029180391592517,1.6864361103335357,0.9041099905770569,-0.8601945846628597,-0.43797424973196464,-1.4306784687160095,0.5305755112030833,1.4332243957418884,-0.9225543720714464,-0.48968272112395295,1.2969905519062221,-0.936011207027195,-1.6967451093902703,-0.7685900450169054,-0.7342364348556424,1.1239395771496394,-0.7346252973511546,1.2214527991637296,-1.2219568417836726,-0.9997034788017629,-1.6861131664061504,-1.1927477447840469,1.0418155557468505,0.8807625994533953,-0.03357751903633732,-0.8118290678921689,0.8108046850909548,-0.5663706526498646,0.7314788056938822,1.2544903710465884,1.9742891069463693,-0.6254827173189841,-0.09333463299772303,-0.020202726976659584,-0.3118013823711032,0.04079223440640133,0.7231970417612443,-0.5343365497940273,-0.1640519436117994,-0.026079552263280893,-2.0141760086038945,-1.509009390294657,0.5384928439241734,-0.7732362655677173,0.6211082673104158,0.1455859735298013,-0.7302821616706046,-2.014175981890958,2.014175973413418,0.2514450496241806,1.4414575166495622,1.4769526331968026,0.44081750801343844,0.07149456117262622,0.24164508024661888,-0.6475184991073684,-0.6022063271601131};
        double[] S_r = new double[] {0.0,0.0,0.0,-0.0,-0.0,0.0,0.0,0.0,0.0,-0.0,-0.0,-0.0,0.0,0.0,-0.0,-0.0,0.0,-0.0,-0.0,-0.0,-0.0,0.0,-0.0,0.0,-0.0,-0.0,-0.0,-0.0,0.0,0.0,-0.0,-0.0,0.0,-0.0,0.0,0.0,0.0,-0.0,-0.0,-0.0,-0.0,0.0,0.0,-0.0,-0.0,-0.0,-0.040615044404649886,-0.0,0.0,-0.0,0.0,0.0,-0.0,-1.0637357541633508,0.9275030757193699,0.0,0.0,0.0,0.0,0.0,0.0,-0.0,-0.0};
        double[] L_r = new double[] {1.7221933429104683,1.4852302684671057,1.3109426132953157,0.9273895832979853,0.7220128001829822,0.8571089845137347,1.2411532998137584,2.3742835164617246,1.852400826808093,1.4648533393978977,0.6205820872339156,0.16847696503800913,0.4659853529981517,1.3131537750142714,1.6975591343675474,1.396506622596097,1.3869551655641779,0.8734270608200048,0.6003315717207102,0.7733817049272755,1.1835876104602845,2.407580861428131,1.7866894734012946,1.5650414284071705,0.5309494116926246,-0.020681401465446836,0.3348682980535801,1.2549453745062544,1.7823998996385595,1.5323615107027546,1.3106893568825573,0.910649527163337,0.6961100595120252,0.8384083380093086,1.2574954572300179,2.084581475178582,1.7070563454926502,1.377042403512547,0.772454988397555,0.4482595933818666,0.6631425871939323,1.2941919509045487,2.3185016506754152,1.7597611460511573,1.5186228615450794,0.5932042017645138,0.09918566964866415,0.41886658277398703,1.2621300721399467,1.3618738353530044,1.2710177796285542,1.2382814325480587,1.0869984784930906,1.0062947991287492,1.058320950867212,1.1930681843105493,2.0840598760261275,1.6878666310673174,1.3975758292616716,0.7561521035329198,0.41270607888881206,0.6385891857001931,1.280289002524297};

        // Input/Output Bag
        // Finally wrap inputBag in tuple
        Tuple inputTuple = tf.newTuple();
        DataBag expected  = bf.newDefaultBag();
        {
        	// Build Input/Output
            inputTuple.append(buildDataBag(ts));
            expected = buildDataBag(ts,L_r,S_r,E_r,false);

        }

        // Initialize Class
        RAD rsvd = new RAD(argsDaily9);
        Schema outputSchema = rsvd.outputSchema(buildInputSchema2());
        DataBag observed = rsvd.exec(inputTuple);
        
        // Test
        if (approximateCompareBags(expected,observed)) {
        	System.out.println("PASS");
        } else {
        	System.out.println("------- EPIC FAIL --------");
        	System.out.println("Expected: "+expected.toString());
        	System.out.println("Observed: "+observed.toString());
        }
        
        assertTrue(approximateCompareBags(expected,observed));

    }
    
    private Boolean approximateCompareBags(DataBag inputBag1, DataBag inputBag2) throws ExecException {
    	
    	// Hardcode Acceptable Error
    	double errorLimit = 0.0000001;
		
		Iterator<Tuple> iter1 = inputBag1.iterator();
		Iterator<Tuple> iter2 = inputBag2.iterator();
		while (iter1.hasNext()) {
			Tuple tuple1 = iter1.next();
			Tuple tuple2 = iter2.next();
			
			// Check error
			if (Math.abs((Double) tuple1.get(0) - (Double) tuple2.get(0)) > errorLimit) return false;
			// TODO: Add unit test for differenced case
			//if (Math.abs((Double) tuple1.get(1) - (Double) tuple2.get(1)) > errorLimit) return false;
			if (Math.abs((Double) tuple1.get(2) - (Double) tuple2.get(2)) > errorLimit) return false;
			if (Math.abs((Double) tuple1.get(3) - (Double) tuple2.get(3)) > errorLimit) return false;
			if (Math.abs((Double) tuple1.get(4) - (Double) tuple2.get(4)) > errorLimit) return false;

		}
    			
    	return true;
    }
    
    private DataBag buildDataBag(double[] obj1) {
    	
        DataBag dataBag  = bf.newDefaultBag();
    	for (int n=0; n<obj1.length; n++) {
        	Tuple newTuple = tf.newTuple();
    	    newTuple.append(obj1[n]);
    	    dataBag.add(newTuple);
    	}
	    return dataBag;
    }

    private DataBag buildDataBag(double[] obj1, double[] obj2, double[] obj3, double[] obj4, boolean isDifferencing) {
        
    	DataBag dataBag  = bf.newDefaultBag();
        double previous = 0.0;

        for (int n=0; n<obj1.length; n++) {
        	Tuple newTuple = tf.newTuple();
    	    newTuple.append(obj1[n]);
    	    if (isDifferencing) {
        	    if (n == 0) {
            	    newTuple.append(0.0);
        	    } else {
            	    newTuple.append(obj1[n] - previous);
        	    }
        	    previous = obj1[n];
    	    } else {
        	    newTuple.append(obj1[n]);
    	    }
    	    newTuple.append(obj2[n]);
    	    newTuple.append(obj3[n]);
    	    newTuple.append(obj4[n]);
    	    dataBag.add(newTuple);
    	}
	    return dataBag;
    }

    private Schema buildInputSchema2() {

		// Outer Tuple Schema
    	List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        fieldSchemas.add(new Schema.FieldSchema("metric"          , DataType.DOUBLE));

    	// Wrap Inner DataBag
        FieldSchema innerTupleFieldSchema = null;
		try {
			innerTupleFieldSchema = new FieldSchema(null, new Schema(fieldSchemas), DataType.TUPLE);
		} catch (FrontendException e) {
			e.printStackTrace();
		}

		// Outer Tuple Schema
    	List<FieldSchema> fieldSchemaFinal = new ArrayList<FieldSchema>();
        try {
			fieldSchemaFinal.add(new Schema.FieldSchema("dummy_bag", new Schema(innerTupleFieldSchema), DataType.BAG));
		} catch (FrontendException e1) {
			e1.printStackTrace();
		}

        // Return Schema
		Schema outputSchema = new Schema(fieldSchemaFinal);
        return outputSchema;

    }


    
}
