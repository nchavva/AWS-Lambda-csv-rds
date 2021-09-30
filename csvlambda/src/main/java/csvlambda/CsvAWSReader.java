package csvlambda;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.opencsv.CSVReader;

public class CsvAWSReader implements RequestHandler<String, String> {
	
	 Regions clientRegion = Regions.DEFAULT_REGION;
     String bucketName = "csvs3file";
     String key = "Samplerows.csv";

     S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
     
     private static final String url = "dbc:mysql://database.csdn9tjc2j4v.ap-south-1.rds.amazonaws.com:3306/sys";
     private static final String user = "admin";
     private static final String pwd = "admin123";
     
 
     
     private static final String SQL_INSERT = "INSERT INTO customers (customer_id, customer_name, city) VALUES (?,?,?)";

	public String handleRequest(String input, Context context) {
		
		try {
			 Connection conn = DriverManager.getConnection(url,user,pwd);
			 PreparedStatement psInsert = conn.prepareStatement(SQL_INSERT);
			
			AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
			         .withRegion(clientRegion)
			         .withCredentials(new ProfileCredentialsProvider())
			         .build();

			 // Get an object and print its contents.
			 System.out.println("Downloading an object");
			 fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
			 System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
			 System.out.println("Content: ");
			// displayTextInputStream(fullObject.getObjectContent());
			// TODO Auto-generated method stub
			 
				CSVReader csvReader = new CSVReader(new InputStreamReader(fullObject.getObjectContent()));
				String[] csvrow =null;
				int recordNo = 0;

				int insrows[];
				// we are going to read data line by line
				while ((csvrow  = csvReader.readNext()) != null) {
					recordNo = recordNo+1;
					psInsert.setLong(1, recordNo);
					psInsert.setString(2, csvrow[1]);
					psInsert.setString(3, csvrow[2]);
					psInsert.addBatch();
					if(recordNo % 1000 ==0)
					{
						 insrows = psInsert.executeBatch();
						System.out.println("total inserted rows" + insrows.length);
					}
				}
				if(recordNo % 1000 !=0)
				{
					 insrows = psInsert.executeBatch();
					System.out.println("total inserted rows" + insrows.length);
				}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		 return "Lambda executed succesfully";
	}
	
    private void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }

}
