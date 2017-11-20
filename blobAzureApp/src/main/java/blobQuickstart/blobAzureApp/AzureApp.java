// MIT License
// Copyright (c) Microsoft Corporation. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE

package blobQuickstart.blobAzureApp;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;
import java.util.Map;
/* *************************************************************************************************************************
 * Summary: This application demonstrates how to use the Blob Storage service.
 * It does so by creating a container, creating a file, then uploading that file, listing all files in a container, 
 * and downloading the file. Then it deletes all the resources it created
 * 
 * Documentation References:
 * Associated Article - https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-java
 * What is a Storage Account - http://azure.microsoft.com/en-us/documentation/articles/storage-whatis-account/
 * Getting Started with Blobs - http://azure.microsoft.com/en-us/documentation/articles/storage-dotnet-how-to-use-blobs/
 * Blob Service Concepts - http://msdn.microsoft.com/en-us/library/dd179376.aspx 
 * Blob Service REST API - http://msdn.microsoft.com/en-us/library/dd135733.aspx
 * *************************************************************************************************************************
 */
public class AzureApp 
{
	/* *************************************************************************************************************************
	 * Instructions: Update the storageConnectionString variable with your AccountName and Key and then run the sample.
	 * *************************************************************************************************************************
	 */
	public static final String storageConnectionString =
	    "DefaultEndpointsProtocol=https;" +
	    "AccountName=<AccountName>;" +
	    "AccountKey=<AccountKey>";
	
	/*
	public static final String storageConnectionString="DefaultEndpointsProtocol=https;AccountName=proxydemoforcntv;AccountKey=ew4tkhiqiJzB07mtE8MD/7GyAcRweFogLqorbqYESDIPSzt0xHKPXCGNH/J6i9wzXgtl2P4US4m/5PJyXe2pKQ==;EndpointSuffix=core.windows.net";
	*/
	
	public static String sendGet(String url, String param) {
        String result = "";
        BufferedReader in = null;
        try {
            String urlNameString = url + "?" + param;
            URL realUrl = new URL(urlNameString);
            URLConnection connection = realUrl.openConnection();
            connection.setRequestProperty("accept", "*/*");
            connection.setRequestProperty("connection", "Keep-Alive");
            connection.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            connection.connect();
            Map<String, List<String>> map = connection.getHeaderFields();
            for (String key : map.keySet()) {
                System.out.println(key + "--->" + map.get(key));
            }
            in = new BufferedReader(new InputStreamReader(
                    connection.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        } catch (Exception e) {
            System.out.println("发送GET请求出现异常！" + e);
            e.printStackTrace();
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

	
    public static void main( String[] args )
    {
    	
    	File sourceFile = null, downloadedFile = null;
        System.out.println("Azure Blob storage quick start sample");
        
    	CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;
        
        try {
        	String MainUrl = "http://asp.cntv.cdnpe.com/";
  			String FileUrl = "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/0.ts";
  			String[] lStr = FileUrl.split("\\.");
  			String fileFormat = lStr[lStr.length-1];
  			String containerName = lStr[0].split("/", 2)[0];
  			String fileName = FileUrl.split("/", 2)[1];
  			System.out.println(containerName);
  			System.out.println(fileFormat);
  			System.out.println(fileName);
  			  			
  			String res = sendGet(MainUrl+FileUrl,"");
  		  
        	// Parse the connection string and create a blob client to interact with Blob storage
        	storageAccount = CloudStorageAccount.parse(storageConnectionString);
	    	blobClient = storageAccount.createCloudBlobClient();
			container = blobClient.getContainerReference(containerName);
	    	
  			// Create the container if it does not exist with public access.
  			System.out.println("Creating container: " + container.getName());
  			container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());		    
  
  			//Creating a sample file
  			sourceFile = File.createTempFile(fileName,"."+fileFormat);
  			System.out.println("Creating a sample file at: " + sourceFile.toString());
  			Writer output = new BufferedWriter(new FileWriter(sourceFile));
  			output.write(res);
  			output.close();
  			
  		    
  			//Getting a blob reference
  			CloudBlockBlob blob = container.getBlockBlobReference(fileName);
  		    
  		    //Creating blob and uploading file to it
  			System.out.println("Uploading the sample file ");
  		    blob.uploadFromFile(sourceFile.getAbsolutePath());

  		    //Listing contents of container
  		    for (ListBlobItem blobItem : container.listBlobs()) {
  		        System.out.println("URI of blob is: " + blobItem.getUri());
  		    }
  		
  			// Download blob. In most cases, you would have to retrieve the reference
  		    // to cloudBlockBlob here. However, we created that reference earlier, and 
  			// haven't changed the blob we're interested in, so we can reuse it. 
  			// Here we are creating a new file to download to. Alternatively you can also pass in the path as a string into downloadToFile method: blob.downloadToFile("/path/to/new/file").
  		    downloadedFile = new File(sourceFile.getParentFile(), "downloadedFile.txt");
  		    blob.downloadToFile(downloadedFile.getAbsolutePath());
  		    
        } 
    	catch (StorageException ex)
		{
  			System.out.println(String.format("Error returned from the service. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
		}
        catch (Exception ex) 
        {
            System.out.println(ex.getMessage());
        }
        finally 
        {
  		    System.out.println("The program has completed successfully.");
  		    System.out.println("Press the 'Enter' key while in the console to delete the sample files, example container, and exit the application.");
		    
		    //Pausing for input
		    Scanner sc = new Scanner(System.in);
		    sc.nextLine();
		    
  		    System.out.println("Deleting the container");
			try {
				if(container != null)
					container.deleteIfExists();
			} catch (StorageException ex) {
	  			System.out.println(String.format("Service error. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
			}
			
  		    System.out.println("Deleting the source, and downloaded files");

			if(downloadedFile != null)
				downloadedFile.deleteOnExit();
						
			if(sourceFile != null)
				sourceFile.deleteOnExit();
		    
		    //Closing scanner
		    sc.close();
        }
      
    }
}
