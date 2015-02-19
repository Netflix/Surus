package org.surus.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.surus.math.AugmentedDickeyFuller;
import org.surus.math.RPCA;

public class RAD extends EvalFunc<DataBag> {

	private final double LPENALTY_DEFAULT_NO_DIFF = 1;
	private final double SPENALTY_DEFAULT_NO_DIFF = 1.4;
	private final double LPENALTY_DEFAULT_DIFF = 1;
	private final double SPENALTY_DEFAULT_DIFF = 1.4;
	
	private final String  colName;
	private final Integer nRows;
	private final Integer nCols;
	private Double  lpenalty;
	private Double  spenalty;
	private Boolean isForceDiff;
	
	private Schema dataBagSchema;
	private final Integer minRecords;
	
	private final Double eps = 1e-12;

	// Constructor
	public RAD(String... parameters) {

		this.colName  = parameters[0];
		this.nCols    = Integer.parseInt(parameters[1]);
		this.nRows    = Integer.parseInt(parameters[2]);
		
		if (parameters.length == 4) {
			this.isForceDiff = Boolean.parseBoolean(parameters[3]);
		} else if (parameters.length != 3) {
			throw new RuntimeException("Invalid parameters list");
		}
		
		// set other parameters
		this.minRecords = 2 * this.nRows;

	}
	
	// Define Output Schema
    @Override
    public Schema outputSchema(Schema input) {

        try {
            if (input.size() != 1) {
                throw new RuntimeException("Expected input to have only a single field");
            }
            
            // Grab Bag Schema
            Schema.FieldSchema inputFieldSchema = input.getField(0);
            if (inputFieldSchema.type != DataType.BAG) {
                throw new RuntimeException("Expected a BAG as input");
            }

            // Check Bag Schema
            Schema inputBagSchema = inputFieldSchema.schema;
            if (inputBagSchema.getField(0).type != DataType.TUPLE) {
                throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
                        DataType.findTypeName(inputBagSchema.getField(0).type)));
            }

            // Define Input Tuple Schema
            this.dataBagSchema = inputBagSchema.getField(0).schema;
            
            this.dataBagSchema.prettyPrint();
            
        	// Create List of Tuple Values
        	List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
        	fieldSchemas.addAll(dataBagSchema.getFields());
            fieldSchemas.add(new Schema.FieldSchema("x_transform", DataType.DOUBLE));
            fieldSchemas.add(new Schema.FieldSchema("rsvd_l", DataType.DOUBLE));
            fieldSchemas.add(new Schema.FieldSchema("rsvd_s", DataType.DOUBLE));
            fieldSchemas.add(new Schema.FieldSchema("rsvd_e", DataType.DOUBLE));

            // Build Tuple and Wrap in DataBag
            FieldSchema tupleFieldSchema = new FieldSchema(null, new Schema(fieldSchemas), DataType.TUPLE);
            FieldSchema bagFieldSchema   = new FieldSchema(this.getClass().getName().toLowerCase().replace(".", "_"), new Schema(tupleFieldSchema), DataType.BAG);
            
            // Return Schema
            Schema outputSchema = new Schema(bagFieldSchema);
            return outputSchema;
            
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

    }
	
    // Helper Function
    public double[][] VectorToMatrix(double[] x, int rows, int cols) {
        double[][] input2DArray = new double[rows][cols];
        for (int n= 0; n< x.length; n++) {
        	int i = n % rows;
        	int j = (int) Math.floor(n / rows);
        	input2DArray[i][j] = x[n];
        }
        return input2DArray;
    }

    // Define Exec
	@Override
	public DataBag exec(Tuple input) throws IOException {
		
		// Hack to get the InputSchema on the backend
		if (this.dataBagSchema == null) {
			this.dataBagSchema = getInputSchema().getField(0).schema.getField(0).schema;
		}

		// Check DataTypes
        if (!(
        		(this.dataBagSchema.getField(this.colName).type == DataType.LONG   ) ||
        		(this.dataBagSchema.getField(this.colName).type == DataType.INTEGER) ||
        		(this.dataBagSchema.getField(this.colName).type == DataType.DOUBLE ) ||
        		(this.dataBagSchema.getField(this.colName).type == DataType.FLOAT  )
        	)) {
        	throw new RuntimeException(String.format("Data type of %s (%s) is not supported,",this.colName,
                    DataType.findTypeName(this.dataBagSchema.getField(this.colName).type)));
        }
		
		// Hardcode getting the bag
		DataBag inputBag = (DataBag) input.get(0);
		
		// Create TupleFactory for Output Bag Generation
		TupleFactory tupleFactory = TupleFactory.getInstance();
		BagFactory   bagFactory   = BagFactory.getInstance();

		// Read Data into Memory
		List<Tuple> tupleList = new ArrayList<Tuple>();
		Iterator<Tuple> bagIter = inputBag.iterator();
		while (bagIter.hasNext()) {
			Tuple tuple = bagIter.next();
			tupleList.add(tuple);
		}
		
		if (tupleList.size() != this.nRows*this.nCols) {
        	throw new RuntimeException("ERROR: this.nRows * this.nCols != tupleList.size()");
		}
		
		// Perform Dickey-Fuller Test
		double[] inputArray = new double[this.nRows*this.nCols];
		Integer numNonZeroRecords = 0;
		for (int n=0; n< inputArray.length; n++) {
			if (this.dataBagSchema.getField(this.colName).type == DataType.DOUBLE) {
				inputArray[n] = (Double) tupleList.get(n).get(this.dataBagSchema.getPosition(this.colName));
			} else if (this.dataBagSchema.getField(this.colName).type == DataType.FLOAT) {
				inputArray[n] = (Float) tupleList.get(n).get(this.dataBagSchema.getPosition(this.colName));
			} else if (this.dataBagSchema.getField(this.colName).type == DataType.LONG ) {
				inputArray[n] = (Long) tupleList.get(n).get(this.dataBagSchema.getPosition(this.colName));
			} else if (this.dataBagSchema.getField(this.colName).type == DataType.INTEGER ) {
				inputArray[n] = (Integer) tupleList.get(n).get(this.dataBagSchema.getPosition(this.colName));
			} else {
	        	throw new RuntimeException(String.format("Data type of %s (%s) is not supported,",this.colName,
	                    DataType.findTypeName(this.dataBagSchema.getField(this.colName).type)));
			}
			
			if (Math.abs(inputArray[n]) > eps) numNonZeroRecords++;
		}
		
		if (numNonZeroRecords>=this.minRecords) {
			AugmentedDickeyFuller dickeyFullerTest = new AugmentedDickeyFuller(inputArray);
			double[] inputArrayTransformed = inputArray;
			if (this.isForceDiff == null && dickeyFullerTest.isNeedsDiff()) {
				// Auto Diff
				inputArrayTransformed = dickeyFullerTest.getZeroPaddedDiff();
			} else if (this.isForceDiff) {
				// Force Diff
				inputArrayTransformed = dickeyFullerTest.getZeroPaddedDiff();
			}
			
			if (this.spenalty == null) {
				this.lpenalty = this.LPENALTY_DEFAULT_NO_DIFF;
				this.spenalty = this.SPENALTY_DEFAULT_NO_DIFF / Math.sqrt(Math.max(this.nCols, this.nRows));
			}

			
			// Calc Mean
			double mean  = 0;
			for (int n=0; n < inputArrayTransformed.length; n++) {
				mean += inputArrayTransformed[n];
			}
			mean /= inputArrayTransformed.length;

			// Calc STDEV
			double stdev = 0;
			for (int n=0; n < inputArrayTransformed.length; n++) {
				stdev += Math.pow(inputArrayTransformed[n] - mean,2) ;
			}
			stdev = Math.sqrt(stdev / (inputArrayTransformed.length - 1));
			
			// Transformation: Zero Mean, Unit Variance
			for (int n=0; n < inputArrayTransformed.length; n++) {
				inputArrayTransformed[n] = (inputArrayTransformed[n]-mean)/stdev;
			}

			// Read Input Data into Array
			// Read Input Data into Array
			double[][] input2DArray = new double[this.nRows][this.nCols];
			input2DArray = VectorToMatrix(inputArrayTransformed, this.nRows, this.nCols);
			
			RPCA rSVD = new RPCA(input2DArray, this.lpenalty, this.spenalty);
			
			double[][] outputE = rSVD.getE().getData();
			double[][] outputS = rSVD.getS().getData();
			double[][] outputL = rSVD.getL().getData();

			// Loop through bag and build output
			DataBag outputBag = bagFactory.newDefaultBag();
			for (int n=0; n< inputArray.length; n++) {

	        	int i = n % this.nRows;
	        	int j = (int) Math.floor(n / this.nRows);

				// Add all previous tuple values
				Tuple oldTuple = tupleList.get(n);
				Tuple newTuple = tupleFactory.newTuple(oldTuple.size() + 4);
				int tupleIndex = 0;
				for (int k = 0; k < oldTuple.size(); k++) {
					newTuple.set(tupleIndex++, oldTuple.get(k));
				}
				
				// TODO: Add additional L,S,E matrices
				newTuple.set(tupleIndex++, inputArrayTransformed[n]);
				newTuple.set(tupleIndex++, outputL[i][j] * stdev + mean);
				newTuple.set(tupleIndex++, outputS[i][j] * stdev);
				newTuple.set(tupleIndex++, outputE[i][j] * stdev);

				// Add Tuple to DataBag
				outputBag.add(newTuple);

			}
			// Return Tuple
			return outputBag;
			
		} else {
			
			// Loop through bag and build output
			DataBag outputBag = bagFactory.newDefaultBag();
			for (int n=0; n< inputArray.length; n++) {

	        	int i = n % this.nRows;
	        	int j = (int) Math.floor(n / this.nRows);

				// Add all previous tuple values
				Tuple oldTuple = tupleList.get(n);
				Tuple newTuple = tupleFactory.newTuple(oldTuple.size() + 4);
				int tupleIndex = 0;
				for (int k = 0; k < oldTuple.size(); k++) {
					newTuple.set(tupleIndex++, oldTuple.get(k));
				}

				// Add Tuple to DataBag
				outputBag.add(newTuple);

			}
			// Return Tuple
			return outputBag;

		}

	}
	
}
