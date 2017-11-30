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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.Scanner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.security.InvalidKeyException;
import java.util.List;
import java.util.Map;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date; 

import java.io.OutputStream;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;

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
	    "AccountName=cntvuswepocstorage03;" +
	    "AccountKey=rOumfJYzEM11KIhY2jwfOcszWE8kopkO9+Ohw/aGzFIrW0YIavNOSZ4/5edrK7IPAf0q67aI++28ijfUJLOiCw==";
	
	/*
	public static final String storageConnectionString="DefaultEndpointsProtocol=https;AccountName=proxydemoforcntv;AccountKey=ew4tkhiqiJzB07mtE8MD/7GyAcRweFogLqorbqYESDIPSzt0xHKPXCGNH/J6i9wzXgtl2P4US4m/5PJyXe2pKQ==;EndpointSuffix=core.windows.net";
	*/
	
    /* 
     * 从输入流中获取字节数组   
     * @param inputStream   
     * @return   
     * @throws IOException   
     */    
    public static  byte[] readInputStream(InputStream inputStream) throws IOException {      
        byte[] buffer = new byte[1024];      
        int len = 0;      
        ByteArrayOutputStream bos = new ByteArrayOutputStream();      
        while((len = inputStream.read(buffer)) != -1) {      
            bos.write(buffer, 0, len);      
        }      
        bos.close();      
        return bos.toByteArray();      
    }   
	/*
	 * download
	 */
	public static byte[] download(String urlString)  
    {
        // 构造URL
        URL url;
        try
        {
            url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection)url.openConnection();      
            //设置超时间为3秒    
            conn.setConnectTimeout(3*1000);    
            //防止屏蔽程序抓取而返回403错误    
            conn.setRequestProperty("User-Agent", "Mozilla/4.0 (compatible; MSIE 5.0; Windows NT; DigExt)");    

             // 打开连接
            URLConnection con = url.openConnection();
            // 输入流
            InputStream is = con.getInputStream();
            byte[] getData = readInputStream(is);
            
            /*
            //文件保存位置    
            File saveDir = new File("test.ts");    
            if(!saveDir.exists()){    
                saveDir.mkdir();    
            }    
            File file = new File("test.ts");        
            FileOutputStream fos = new FileOutputStream(file);         
            fos.write(getData);     
            if(fos!=null){    
                fos.close();      
            }    
            */
            if(is!=null){    
                is.close();    
            }    
            
            return getData;
        }
        catch (MalformedURLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
		return null;
    }   
	
	/*
	 * 发送Http Get请求
	 * @param url 请求的链接
	 * @param param 请求的参数
	 * 
	 * @output result Get请求返回的参数 
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

	/*
	 * 由url得到blob相关参数
	 * @param fileUrl 是除去域名以外的链接
	 * 
	 * @output result 分别代表blob的container名字，blob文件名字以及blob文件的格式
	 */
	public static String[] getParam(String fileUrl)
	{
		String[] result = new String[3];
		//String FileUrl = "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/0.ts";
		String[] lStr = fileUrl.split("\\.");
		result[0] = lStr[0].split("/", 2)[0];
		result[1] = fileUrl.split("/", 2)[1];
		result[2] = lStr[lStr.length-1];
		return result;
	}
	
	/*
	 * 上传文件
	 * @param storageConnectString Azure Blob的连接字符串
	 * @param mainUrl = "http://asp.cntv.cdnpe.com/"
	 * @param fileUrl 除去域名外的链接部分，用来创建blob container名字以及文件名字
	 */
	public static void uploadFile(String storageConnectionString, String mainUrl, String fileUrl) throws Throwable, IOException
	{
    	CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;

    	String[] result = getParam(fileUrl);	
    	byte[] res = download(mainUrl+fileUrl);
		//String res = sendGet(mainUrl+fileUrl,"");
		// 创建blob client以及container
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
		container = blobClient.getContainerReference(result[0]);
		System.out.println("Creating container: " + container.getName());
		container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());		    		
	    
		//创建blob
		CloudBlockBlob blob = container.getBlockBlobReference(result[1]);
		
	    blob.uploadFromByteArray(res, 0, res.length);
	}
	
	/*
	 * 批量上传文件
	 * @param storageConnectString Azure Blob的连接字符串
	 * @param mainUrl = "http://asp.cntv.cdnpe.com/"
	 * @param fileUrls 除去域名外的链接部分的列表，用来批量创建blob container名字以及文件名字
	 */
	public static void uploadBatchFile(String storageConnectionString, String mainUrl, String[] fileUrls) throws IOException, Throwable
	{
		for (String str: fileUrls)
		{
			uploadFile(storageConnectionString, mainUrl, str);
		}
	}
	
	/*
	 * 删除单个文件
	 * @param storageConnectString Azure Blob的连接字符串
	 * @param fileUrl 除去域名外的链接部分的列表
	 */
	public static void deleteFile(String storageConnectionString, String fileUrl) throws InvalidKeyException, URISyntaxException, StorageException
	{
		CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;

    	String[] result = getParam(fileUrl);
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
		container = blobClient.getContainerReference(result[0]);
		CloudBlockBlob blob = container.getBlockBlobReference(result[1]);
	    blob.deleteIfExists();
	}
	
	/*
	 * 删除目录
	 * @param storageConnectString Azure Blob的连接字符串
	 * @param DirUrl 目录绝对链接 "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/"
	 */
	public static void deleteDir(String storageConnectionString, String DirUrl) throws InvalidKeyException, URISyntaxException, StorageException
	{
		CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;
    	CloudBlob blob = null;

    	String[] result = getParam(DirUrl);
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
		container = blobClient.getContainerReference(result[0]);
		//blobDirectory = container.getDirectoryReference(result[1]);
		for (ListBlobItem item: container.listBlobs(result[1]))
		{
			if (item instanceof CloudBlob) {
	            blob = (CloudBlob) item;
			}
		    if (blob.deleteIfExists()) {
		        System.out.println("Delete file "+blob.getUri().toString());
		    }
		}
	}
	
	/*
	 * 删除某一时刻之前的文件
	 * @param storageConnectString Azure Blob的连接字符串
	 * @param date 时刻
	 */
	public static void deleteFilesBeforeTime(String storageConnectionString, Date date) throws InvalidKeyException, URISyntaxException, StorageException
	{
		CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlob blob = null;

    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
    	for(CloudBlobContainer container:blobClient.listContainers()) {
    		for(ListBlobItem item: container.listBlobs()) {
    			if (item instanceof CloudBlob) {
    	            blob = (CloudBlob) item;
    			}
    			Date fileDate = blob.getProperties().getLastModified();
    			if(date.getTime()<fileDate.getTime())
    			{
    				blob.deleteIfExists();
    			}
    		}
    	}
	}
		
	/*
	 * 查询目录
	 * @param storageConnectString Azure Blob的连接字符串
	 * @param DirUrl 目录绝对链接 "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/"
	 */
	public static void listFileInDir(String storageConnectionString, String DirUrl) throws InvalidKeyException, URISyntaxException, StorageException
	 {
		CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;

    	String[] result = getParam(DirUrl);
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
		container = blobClient.getContainerReference(result[0]);
		CloudBlobDirectory blobDirectory = container.getDirectoryReference(result[1]);
	    //Listing contents of container
	    for (ListBlobItem blobItem : blobDirectory.listBlobs()) {
	        System.out.println("URI of blob is: " + blobItem.getUri());
	    }
	 }
	
	
	/*
	 * 下载文件（查询文件）
	 * @param storageConnectString Azure Blob的连接字符串
	 * @param fileUrl 除去域名外的链接部分的列表，用来批量创建blob container名字以及文件名字
	 * @param localFileDir 下载文件目录路径
	 * @param localFileName 下载文件名称
	 */
	public static void downloadFile(String storageConnectionString, String fileUrl, String localFileDir, String localFileName) throws StorageException, IOException, URISyntaxException, InvalidKeyException
	{
    	CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;
    	String[] result = getParam(fileUrl);
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
		container = blobClient.getContainerReference(result[0]);
		CloudBlockBlob blob = container.getBlockBlobReference(result[1]);
		
		File downloadedFile = null;
		downloadedFile = new File(localFileDir, localFileName);
		blob.downloadToFile(downloadedFile.getAbsolutePath());
	}
	
    /*
     * 设置读取权限(设置为根据blob的路径可以匿名访问blob)
     * @param storageConnectString Azure Blob的连接字符串
	 * @param containerName blob container名字
     */
	public static void setPermission(String storageConnectionString, String containerName) throws InvalidKeyException, URISyntaxException, StorageException
	{
		CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
    	container = blobClient.getContainerReference(containerName);
    	BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
    	containerPermissions.setPublicAccess(BlobContainerPublicAccessType.BLOB);
    	container.uploadPermissions(containerPermissions);
	}
	
	/*
	 * append blob String
	 */
	public static void appendString(String storageConnectionString, String containerName, String blobName, String appendContent) throws InvalidKeyException, URISyntaxException, StorageException, IOException
	{
		CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container = null;
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
    	container = blobClient.getContainerReference(containerName);
    	container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());		    		
	    
    	CloudAppendBlob blob = container.getAppendBlobReference(blobName);
    	if (blob.exists())
    	{
        	blob.appendText(appendContent);
    	}else {
    		blob.createOrReplace();
    	}
	}
	
	/*
	 * append blob byte[]
	 */
	public static void appendBytes(String storageConnectionString, String containerName, String blobName, byte[] appendContent) throws InvalidKeyException, URISyntaxException, StorageException, IOException
	{
		CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container = null;
    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
    	blobClient = storageAccount.createCloudBlobClient();
    	container = blobClient.getContainerReference(containerName);
    	container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());		    		
	    
    	CloudAppendBlob blob = container.getAppendBlobReference(blobName);
    	if (blob.exists())
    	{
        	blob.appendFromByteArray(appendContent, 0, appendContent.length);
    	}else {
    		blob.createOrReplace();
    	}
	}
	
	/*
	 * read appendblob byte[]
     * @param storageConnectString Azure Blob的连接字符串
	 * @param containerName blob container名字
	 * @param blobName blob 名字
	 * @param startPos 读取开始位置
	 * @param offsite 读取结束位置
	 * return byte[]
	 */
	public static byte[] readBytes(String storageConnectionString, String containerName, String blobName, long startPos, long offsite) throws InvalidKeyException, URISyntaxException, StorageException, IOException
	{
		try {
			CloudStorageAccount storageAccount;
	    	CloudBlobClient blobClient = null;
	    	CloudBlobContainer container = null;
	    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
	    	blobClient = storageAccount.createCloudBlobClient();
	    	container = blobClient.getContainerReference(containerName);
	    	container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());		    		
		    
	    	CloudAppendBlob blob = container.getAppendBlobReference(blobName);
	    	if (blob.exists())
	    	{
	    		byte[] buffer = new byte[1024];
	        	blob.downloadRangeToByteArray(startPos, offsite, buffer, 0);
	        	return buffer;
	    	} else {
	    		blob.createOrReplace();
	    	}
	    	return null;
		} 
    	catch (StorageException ex)
		{
  			System.out.println(String.format("Error returned from the service. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
			return null;
        }
	}
	
	/*
	 * 获取blob读取的流
     * @param storageConnectString Azure Blob的连接字符串
	 * @param containerName blob container名字
	 * @param blobName blob 名字
	 * @output BlobInputStream blob的stream信息
	 */
	
	public static BlobInputStream getStream(String storageConnectionString, String containerName, String blobName) throws InvalidKeyException, URISyntaxException
	{
		try {
			CloudStorageAccount storageAccount;
	    	CloudBlobClient blobClient = null;
	    	CloudBlobContainer container = null;
	    	storageAccount = CloudStorageAccount.parse(storageConnectionString);
	    	blobClient = storageAccount.createCloudBlobClient();
	    	container = blobClient.getContainerReference(containerName);
	    	container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(), new OperationContext());		    		
		    
	    	CloudAppendBlob blob = container.getAppendBlobReference(blobName);
	    	if (blob.exists())
	    	{
	    		return blob.openInputStream();
	    	} else {
	    		blob.createOrReplace();
	    	}
	    	return null;
		} 
    	catch (StorageException ex)
		{
  			System.out.println(String.format("Error returned from the service. Http code: %d and error code: %s", ex.getHttpStatusCode(), ex.getErrorCode()));
			return null;
        }
	}
	
	/*
	 * 获取blob读取的流
     * @param storageConnectString Azure Blob的连接字符串
	 * @param containerName blob container名字
	 * @param blobName blob 名字
	 * @param Skip 跳过多少bytes
	 * @output BlobInputStream blob的stream信息
	 */
	
	public static BlobInputStream getStream(String storageConnectionString, String containerName, String blobName, long Skip) throws InvalidKeyException, URISyntaxException, IOException
	{
		BlobInputStream stream = getStream(storageConnectionString, containerName, blobName);
		stream.skip(Skip);
		return stream;
	}
	
	public static void main( String[] args ) throws Throwable
    {
    	
    	File sourceFile = null, downloadedFile = null;
        System.out.println("Azure Blob storage quick start sample");
        
    	CloudStorageAccount storageAccount;
    	CloudBlobClient blobClient = null;
    	CloudBlobContainer container=null;
        
        try {
        	String url = "http://asp.cntv.cdnpe.com/asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/0.ts";
        	String MainUrl = "http://asp.cntv.cdnpe.com/";
  			String FileUrl = "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/0.ts";
  			String DirUrl = "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/";
  			String[] FileUrls = {
  			                     "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/0.ts",
  			                     "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/1.ts",
  			                     "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/2.ts",
  			                     "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/3.ts",
  			                     "asp/hls/450/0303000a/3/default/44ce409f6c5f49028fd6921fe0cdee6d/4.ts"
  			};
  			setPermission(storageConnectionString, "asp");
  			//uploadFile(storageConnectionString, MainUrl, FileUrl);  			
  			//listFileInDir(storageConnectionString, DirUrl);
  			//deleteFile(storageConnectionString, FileUrl);
  			//uploadBatchFile(storageConnectionString, MainUrl, FileUrls);
  			//listFileInDir(storageConnectionString, DirUrl);
  			//deleteDir(storageConnectionString, DirUrl);
  			//listFileInDir(storageConnectionString, DirUrl);
  			appendString(storageConnectionString, "asp", "log", "test1");
  			appendString(storageConnectionString, "asp", "log", "test2");
  			appendString(storageConnectionString, "asp", "log", "test3");
  			appendString(storageConnectionString, "asp", "log", "test4");
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
        }
      
    }
}
