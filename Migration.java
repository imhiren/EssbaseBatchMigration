import com.essbase.api.base.*;
import com.essbase.api.datasource.*;
import com.essbase.api.datasource.IEssCube.EEssCubeType;
import com.essbase.api.datasource.EssOutlineEditOption;
import com.essbase.api.datasource.IEssOlapApplication.EEssDataStorageType;
import com.essbase.api.domain.*;
import com.essbase.api.metadata.IEssCubeOutline;
import com.essbase.api.session.*;
import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;

public class Migration {

	private String providerURL, serverName , username, password, applicationName, cubeName, serverOS, sharedPath, applicationType, pathSeperator;
	private String javaSharedPath;
	EEssDataStorageType applicationStorageType;
	EEssCubeType cubeType;
	boolean allowDuplicateMembers;
	private IEssOlapServer olapSvr;
	private IEssOlapApplication application;
	private IEssCube cube;
	
	Migration(String URL, String clusterName, String user, String pass, String app, String cube, String OS, String path){
		this.providerURL = URL; 
		this.serverName = clusterName;
		this.username = user;
		this.password = pass;
		this.applicationName = app;
		this.cubeName = cube;
		this.serverOS = OS;
		this.sharedPath = path;
		this.setPathSeperator();
	}
	
	public static void main(String args[]) {
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));		
		
		System.out.println("Enter 1 to download \nEnter 2 to upload");
		int option = 0;
		
		try{
			option = Integer.parseInt(br.readLine());
		}
		catch(IOException e){
			log(4, "Error while IO Operation. Force Stop.");
			System.exit(0);
		}
		
		if(option == 1) {
			//Fetch the source application details from CSV files
			ArrayList<Migration[]> list = getList();
			
			log(1, "Total no. of databases present in list: " + list.size());
			int track = 1;
			
			for(Migration[] pair : list) {	
				
				Migration source = pair[0];
				log(2, "Processing cube " + source.getUniqueName() + track + " of " + list.size());
				log(2, source.getUniqueName() + "****** Start Download *********");
				
				if(!source.connect(false)) {
					log(4 , "Connection error. Skiping cube " + source.getUniqueName() + track + " of " + list.size());
					System.out.println();
					track++;
					continue;
				}
				
				if(!downloadCubeObjectsUsingFiles(source)) {
					log(4 , "Error while downloading artifacts for " + source.getUniqueName() + track + " of " + list.size());
					track++;
					continue;
				}
				
				source.disconnect();
				
				log(2, source.getUniqueName() + "****** End Download ***********)");
				log(2, "Cube " + source.getUniqueName() + track + " of " + list.size() + " processed successfully");
				System.out.println();
				track++;
			}
			
		}
		else if (option == 2) {
			//Fetch the source application details from CSV files
			ArrayList<Migration[]> list = getList();
			
			log(1, "Total no. of databases present in list: " + list.size());
			int track = 1;
			
			for(Migration[] pair : list) {
				
				Migration source = pair[0];
				Migration destination = pair[1];
				log(2, "Processing cube " +  source.applicationName + "." + source.cubeName + " "+ track + " of " + list.size());
				log(2, "****** Start Uploading [" + source.applicationName + "." + source.cubeName +  "] *********");
				
				if(!source.connect(false)) {
					log(4 , "Connection error. Skiping cube " + source.getUniqueName() + track + " of " + list.size());
					System.out.println();
					track++;
					continue;
				}
				
				//destination details server setup
				destination.applicationName = source.applicationName;
				destination.cubeName = source.cubeName;
				destination.applicationStorageType = source.applicationStorageType;
				destination.cubeType = source.cubeType;
				destination.allowDuplicateMembers = source.isCubeMemberNamesNonUnique();
				destination.applicationType = source.getApplicationType();
				destination.javaSharedPath = source.sharedPath;
				
				if(!destination.connect(true)) {
					log(4 , "Connection error. Skiping cube " + destination.getUniqueName() + track + " of " + list.size());
					track++;
					continue;
				}
				
				uploadCubeObjectsUsingFiles(source, destination);
				
				source.disconnect();
				destination.disconnect();
				
				log(2, "****** Stop Uploading *********");
				log(2, "Cube " + destination.getUniqueName() + track + " of " + list.size() + " processed successfully");
				System.out.println();
				track++;
			}
		}
		else {
			System.out.println("Invalid option entered.");
		}
	}
	
	public boolean executeMaxl() {
		try {
			IEssMaxlSession maxlSession = this.olapSvr.openMaxlSession("Export Data");
			boolean result = maxlSession.execute("export database'EssApp04'.'MAT' all data in columns to server data_file '/u01/Oracle/WinDrive/backup/EssApp04/MAT/MAT.txt';");
			log(3, "Result: " + result);
			ArrayList<String> mes = maxlSession.getMessages();
			
			for(String m : mes) {
				System.out.println(m);
			}
			maxlSession.close();
			return result;
		} catch (EssException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		
	}
	
	public void disconnect() {
		try {
			this.cube.stop();
			log(1, "Stopped cube [" + this.cubeName + "]");
		}
		catch(EssException e) {
			log(4, "Error while stopping the cube.");
		}
		
		try {
			this.application.stop();
			log(1, "Stopped application [" + this.applicationName + "]");
		}
		catch(EssException e) {
			log(4, "Error while stopping the application.");
		}
	}
	
	public boolean connect(boolean create) {
		if(create) {
			if(!this.connectToServer()) {
				log(4, "Cannot connect to server to create new application.");
				log(4, "	Server URL:" + this.providerURL);
				log(4, "	Server Name:" + this.serverName);
				return false;
			}
			if(!this.createNewApplication()) {
				log(4, "Cannot create/get new Application handle.");
				log(4, "	Server URL:" + this.providerURL);
				log(4, "	Server Name:" + this.serverName);
				log(4, "	Application Name:" + this.applicationName);
				return false;
			}
			if(!this.createNewCube()) {
				log(4, "Cannot create/get new Cube handle.");
				log(4, "	Server URL:" + this.providerURL);
				log(4, "	Server Name:" + this.serverName);
				log(4, "	Application Name:" + this.applicationName);
				log(4, "	Cube Name:" + this.cubeName);
				return false;
			}
		}
		else {
			if(!this.connectToServer()) {
				log(4, "Cannot cconnect to server.");
				log(4, "	Server URL:" + this.providerURL);
				log(4, "	Server Name:" + this.serverName);
				return false;
			}
			if(!this.connectToApplication()) {
				log(4, "Cannot connect to application.");
				log(4, "	Server URL:" + this.providerURL);
				log(4, "	Server Name:" + this.serverName);
				log(4, "	Application Name:" + this.applicationName);
				return false;
			}
			if(!this.connectToCube()) {
				log(4, "Cannot connect to Cube.");
				log(4, "	Server URL:" + this.providerURL);
				log(4, "	Server Name:" + this.serverName);
				log(4, "	Application Name:" + this.applicationName);
				log(4, "	Cube Name:" + this.cubeName);
				return false;
			}
		}
		return true;
	}
	
	public boolean connectToServer(){
		try {
	        IEssbase ess = IEssbase.Home.create(IEssbase.JAPI_VERSION);	        
	    	// Sign On to the Provider
	        IEssDomain dom = ess.signOn(this.username, this.password, false, null, this.providerURL);
	        // Open connection with OLAP server and get the cube.
	        this.olapSvr = dom.getOlapServer(this.serverName);
	        this.olapSvr.connect();
	        log(1, "Connection established with server: " + this.olapSvr.getName() + " (" + this.providerURL + ")");
	        return true;
		}
		catch(EssException e) {
			log(4, "Failed to establish a connection to server");
			log(4, e.getMessage());
			return false;
		}
	}
	
	
	
	public boolean connectToApplication() {
		try {
			this.application = this.olapSvr.getApplication(this.applicationName);
			this.applicationStorageType = this.application.getDataStorageType();
			log(1, "Connected to application: " + this.application.getName());
			return true;
		}
		catch(EssException e) {
			log(4, "Failed connecting to application: " + this.applicationName);
			log(4, e.getMessage());
			return false;
		}
	}
	
	public boolean connectToCube() {
		try {
			this.cube = this.application.getCube(this.cubeName);
			this.cubeType = this.cube.getCubeType();
			log(1, "Connected to cube: " + this.cube.getName());
			return true;
		}
		catch(EssException e) {
			log(4, "Failed connecting to cube: " + this.cubeName);
			log(4, e.getMessage());
			return false;
		}
	}
	
	public boolean createNewApplication() {
		try{
			if(this.applicationStorageType.intValue() == IEssOlapApplication.EEssDataStorageType.ASO_INT_VALUE) {
				log(2, "Creating new ASO application");
				this.application = this.olapSvr.createApplication(this.applicationName, (short) 4, this.applicationType); // Only valid parameter is 4 which is used to create application with DataStorage as ASO 
			}
			else {
				log(2, "Creating new BSO application");
				this.application = this.olapSvr.createApplication(this.applicationName, this.applicationType); // create application with default storage type
			}
			log(1, "New application created : " + this.application.getName());
			return true;
		}
		catch(EssException e){
			log(4, "Failed to create a new application: " + this.applicationName);
			log(4, e.getMessage());
			//e.printStackTrace();
			return this.connectToApplication();
		}
	}
	
	public boolean createNewCube() {
		try {
			log(2, "Creating new cube of type: " + this.cubeType.stringValue() + ", isUnicode: " + this.allowDuplicateMembers);
			this.cube = this.application.createCube(this.cubeName, this.cubeType, this.allowDuplicateMembers);
			log(1, "New cube created : " + this.cube.getName());
			return true;
		}
		catch(EssException e) {
			log(4, "Failed to create a new cube.");
			log(4, e.getMessage());
			return this.connectToCube();
		}
	}
	
	public static boolean copyCubeObjectsUsingBytes(Migration sourceServer, Migration destinationServer) {
		try {
			destinationServer.createSharedPathFolders(false);
        	IEssIterator objectIterator = sourceServer.olapSvr.getOlapFileObjects(sourceServer.applicationName, sourceServer.cubeName, IEssOlapFileObject.TYPE_ALL);
			IEssBaseObject objects[] = objectIterator.getAll();
			for(IEssBaseObject obj : objects){
				IEssOlapFileObject fileobj = (IEssOlapFileObject) obj; // :D
				String filename = fileobj.getName();
				int filetype = fileobj.getType();
				try {
					lock(sourceServer, filetype, filename);
					byte file[] = sourceServer.cube.copyOlapFileObjectFromServer(filetype, filename, false);
					unlock(sourceServer, filetype, filename);
					
					lock(destinationServer, filetype, filename);
					if(filetype == 1) {
						//Outline file workaround
						lock(destinationServer, 2048, filename);
						destinationServer.cube.copyOlapFileObjectToServer(2048, filename, file, true); // Copy as Text
						log(3, "Outline File exported as: " + destinationServer.sharedPath + filename + ".txt");
						destinationServer.cube.copyOlapFileObjectToServer(filetype, filename, destinationServer.sharedPath + filename + ".txt",false);
						destinationServer.cube.setActive();
						destinationServer.cube.restructure((short) 2);
					}
					else {
						destinationServer.cube.copyOlapFileObjectToServer(filetype, filename, file, true);
					}
					log(1, "	File Copied: " + filename + " | File Type:" + filetype);
					unlock(destinationServer, filetype, filename);
					
				}
				catch(EssException e) {
					log(4, "	Copy Failed: " + filename + " | File Type:" + filetype);
					log(4, e.getMessage());
				}
				finally{
					unlock(sourceServer, filetype, filename);
					unlock(destinationServer, filetype, filename);
				}
			}
        }
        catch(Exception e) {
			log(4, e.getMessage());
        	e.printStackTrace();
        }
		return true;
	}
	
	
	
	
	public static boolean copyCubeObjectsUsingFiles(Migration sourceCube, Migration destinationCube) {
		
		if(!sourceCube.createSharedPathFolders(true)) {
			log(3, "Path: " + sourceCube.sharedPath);
			log(4, "Failed to get access to shared path of Source Cube. Forcee Stop.");
			System.exit(0);
			return false;
		}
		
		if(!destinationCube.createSharedPathFolders(false)) {
			log(3, "Path: " + sourceCube.sharedPath);
			log(4, "Failed to get access to shared path of Destination Cube.");
			System.exit(0);
			return false;
		}
		
		try {			
        	IEssIterator objectIterator = sourceCube.olapSvr.getOlapFileObjects(sourceCube.applicationName, sourceCube.cubeName, IEssOlapFileObject.TYPE_ALL);
			IEssBaseObject objects[] = objectIterator.getAll();
			for(IEssBaseObject obj : objects){
				IEssOlapFileObject fileobj = (IEssOlapFileObject) obj; // :D
				String filename = fileobj.getName();
				int filetype = fileobj.getType();
				//Skip files like .esm etc (Essbase Error(1051041): Insufficient privilege for this operation)
				if(filetype == 8192) {
					continue;
				}
				String fileExtension = getFileExtension(filetype);
				try {
					//Download File form Source server
					lock(sourceCube, filetype, filename);
					sourceCube.cube.copyOlapFileObjectFromServer(filetype, filename, sourceCube.sharedPath + filename + fileExtension,true);
					unlock(sourceCube, filetype, filename);
					
					//Upload File destination server
					lock(destinationCube, filetype, filename);
					if(filetype == 1) {
						//Outline file needs restructure
						destinationCube.cube.copyOlapFileObjectToServer(filetype, filename, destinationCube.sharedPath + filename + fileExtension, true);
						destinationCube.cube.setActive();
						destinationCube.cube.restructure((short) 0); //Parameter 0 for Restructure & keep all data intact - IEssCube.EEssRestructureOption.KEEP_ALL_DATA_INT_VALUE 
					}
					else {
						destinationCube.cube.copyOlapFileObjectToServer(filetype, filename, destinationCube.sharedPath + filename + fileExtension, true);
					}
					log(1, "	File Copied: " + filename + " | File Type:" + fileExtension);
					unlock(destinationCube, filetype, filename);
				}
				catch(EssException e) {
					log(4, "	Copy Failed: " + filename + " | File Type:" + fileExtension);
					log(4, " " + e.getMessage());
				}
				finally{
					unlock(sourceCube, filetype, filename);
					unlock(destinationCube, filetype, filename);
				}
			}
        }
        catch(Exception e) {
        	log(4, "Failed while copying artifacts.");
			log(4, e.getMessage());
        	e.printStackTrace();
        	return false;
        }
		return true;
	}
	
	public static boolean downloadCubeObjectsUsingFiles(Migration sourceCube) {	
		if(!sourceCube.createSharedPathFolders(true)) {
			log(4, "Failed to access shared path of " + sourceCube.getUniqueName());
			return false;
		}
		
		try {
			log(2, "Start downloading of artifacts from " + sourceCube.getUniqueName());
        	IEssIterator objectIterator = sourceCube.olapSvr.getOlapFileObjects(sourceCube.applicationName, sourceCube.cubeName, IEssOlapFileObject.TYPE_ALL);
			IEssBaseObject objects[] = objectIterator.getAll();
			double totalFile = 0;
			double fileTracker = 0;
			
			//Count total number of required files
			for(IEssBaseObject obj : objects){
				IEssOlapFileObject fileobj = (IEssOlapFileObject) obj; // :D
				int filetype = fileobj.getType();
				//skip files which required extra privileges
				if(filetype == 8192) {
					continue;
				}
				else{
					totalFile++;
				}
			}
			
			for(IEssBaseObject obj : objects){
				IEssOlapFileObject fileobj = (IEssOlapFileObject) obj; // :D
				String filename = fileobj.getName();
				int filetype = fileobj.getType();
				long filesize = fileobj.getFileSizeLong();
				String fileExtension = getFileExtension(filetype);
				fileTracker++;
				
				//Skip files like .esm etc (Essbase Error(1051041): Insufficient privilege for this operation)
				if(filetype == 8192) {
					continue;
				}				
				
				try {
					lock(sourceCube, filetype, filename);
					sourceCube.cube.copyOlapFileObjectFromServer(filetype, filename, sourceCube.sharedPath + filename +  fileExtension,true);
					unlock(sourceCube, filetype, filename);
					log(1, "	[" + Math.round((fileTracker / totalFile) * 100.0) + "%] File Downloaded " + sourceCube.getUniqueName() + filename + "." + fileExtension + " Size: " + filesize + " bytes");
				}
				catch(EssException e) {
					log(4, "	[" + Math.round((fileTracker / totalFile) * 100.0) + "%] Download failed " + sourceCube.getUniqueName() + filename + "." + fileExtension + " Size: " + filesize + " bytes");
					log(4, "         " + e.getMessage());
				}
				finally {
					unlock(sourceCube, filetype, filename);
				}
			}
			
			log(2, "Downloading of artifacts from " + sourceCube.getUniqueName() + " complete");
			return exportData(sourceCube);
        }
        catch(Exception e) {
        	log(4, "Failed while downloading artifacts.");
			log(4, e.getMessage());
        	e.printStackTrace();
        	return false;
        }
	}
	
	public static boolean uploadCubeObjectsUsingFiles(Migration sourceCube, Migration destinationCube) {
		
		if(!destinationCube.createSharedPathFolders(false)) {
			log(4, "Failed to get access to shared path of Source Cube.");
			return false;
		}
		
		try {
			log(2, "Start uploading artifacts to " + destinationCube.applicationName + "." + destinationCube.cubeName + "(" + destinationCube.providerURL + ")");
			//get list of source filenames and types
	    	IEssIterator objectIterator = sourceCube.olapSvr.getOlapFileObjects(sourceCube.applicationName, sourceCube.cubeName, IEssOlapFileObject.TYPE_ALL);
			IEssBaseObject objects[] = objectIterator.getAll();
			double totalFile = 0;
			double fileTracker = 0;
			
			
			//Count total number of required files
			for(IEssBaseObject obj : objects){
				IEssOlapFileObject fileobj = (IEssOlapFileObject) obj; // :D
				int filetype = fileobj.getType();
				//skip files which required extra privileges
				if(filetype == 8192) {
					continue;
				}
				else{
					totalFile++;
				}
			}
			
			
			for(IEssBaseObject obj : objects){
				IEssOlapFileObject fileobj = (IEssOlapFileObject) obj; // :D
				String filename = fileobj.getName();
				int filetype = fileobj.getType();
				long filesize = fileobj.getFileSizeLong();
				String fileExtension = getFileExtension(filetype);
				fileTracker++; 
				
				//Skip files like .esm etc (Essbase Error(1051041): Insufficient privilege for this operation)
				if(filetype == 8192) {
					continue;
				}
								
				try {			
					//Upload File destination server
					lock(destinationCube, filetype, filename);
					if(filetype == 1) {
						//Outline file needs restructure
						destinationCube.cube.copyOlapFileObjectToServer(filetype, filename, destinationCube.sharedPath + filename +  fileExtension, true);
						destinationCube.cube.setActive();
						destinationCube.cube.restructure((short) 0); //Parameter 0 for Restructure & keep all data intact - IEssCube.EEssRestructureOption.KEEP_ALL_DATA_INT_VALUE 
					}
					else {
						destinationCube.cube.copyOlapFileObjectToServer(filetype, filename, destinationCube.sharedPath + filename +  fileExtension, true);
					}
					log(1, "	[" + Math.round((fileTracker / totalFile) * 100.0) + "%]File Uploaded: " + destinationCube.getUniqueName() + filename + "." + fileExtension + " Size: " + filesize + " bytes");
					unlock(destinationCube, filetype, filename);
				}
				catch(EssException e) {
					log(4, "	[" + Math.round((fileTracker / totalFile) * 100.0) + "%]Upload Failed: " + destinationCube.getUniqueName() + filename + "." + fileExtension + " Size: " + filesize + " bytes");
					log(4, "         " + e.getMessage());					
				}
				finally {
					unlock(destinationCube, filetype, filename);
				}
			}
			log(2, "Uploading artifacts to " + destinationCube.applicationName + "." + destinationCube.cubeName + " complete");
			return importData(destinationCube);
			
	    }
	    catch(Exception e) {
	    	log(4, "Failed while uploading artifacts.");
			log(4, e.getMessage());
	    	e.printStackTrace();
	    	return false;
	    }
	}

	public static boolean exportData(Migration server) {
		try {
			String path = server.sharedPath;
			
			log(2, "Starting to export data from " + server.getUniqueName());
			log(3, "Exporting to: " + path + server.cubeName + ".txt");
			log(1, "Program will wait until export is complete");
			
			if(server.applicationStorageType.intValue() == IEssOlapApplication.EEssDataStorageType.ASO_INT_VALUE) {
				log(2, "ASO Application - exporting only lev 0");
				server.cube.exportData(path + server.cubeName + ".txt", IEssCube.EEssDataLevel.LEVEL0, false); //ASO App only lev 0 and column format set to false
			}
			else {
				log(2, "BSO Application - exporting all level data in column format");
				server.cube.exportData(path + server.cubeName + ".txt", IEssCube.EEssDataLevel.ALL, true); //ASO App only lev 0 and column format set to false
			}
			
			int files = 0;
			while(files < 100) {
				String checkFile;
				
				if(files == 0) {
					checkFile = path + server.cubeName + ".txt";
				}
				else {
					checkFile = path + server.cubeName + "_" + files + ".txt";
				}
				
				File tempFile = new File(checkFile);
				
				if(tempFile.exists()) {
					files++;
				}
				else {
					break;
				}
			}
			
			log(2 , "Export from " + server.getUniqueName() + " complete");
			log(1, server.getUniqueName() + " Data exported in " + (files) + " file(s)");
			log(1, server.getUniqueName() + " Lev 0 blocks: " + server.cube.getNonMissingLeafBlocks());
			log(1, server.getUniqueName() + " Parent blocks: " + server.cube.getNonMissingNonLeafBlocks());
			return true;
		}
		catch(EssException e) {
			log(4, "Failed to export data for application: " + server.applicationName);
			log(4, " " + e.getMessage());
			return false;
		}
	}
	
	public static boolean importData(Migration server) {
		try {
			//if the path is not in UNC form
			//String path = server.javaSharedPath + "/" + server.applicationName + "/" + server.cubeName + "/" ;
			
			//If path is UNC 
			String path = server.sharedPath;
			
			log(2, "Starting to import data to " + server.getUniqueName());
			
			int files = 0;
			while(files < 100) {
				String checkFile;
				
				if(files == 0) {
					checkFile = path + server.cubeName + ".txt";
				}
				else {
					checkFile = path + server.cubeName + "_" + files + ".txt";
				}
				
				File tempFile = new File(checkFile);
				
				if(tempFile.exists()) {
					files++;
				}
				else {
					//log(3, "File not found: " + checkFile);
					break;
				}
			}
			
			log(1, "Total no. of data files to be imported: " + (files));
			//no import files skip loop
			
			if(files == 0) {
				files = -1;
			}
			
			//switch back to source path
			path = server.sharedPath;
			
			for (int i = 0; i < files; i++) {
				
				String checkFile;
				
				if(i == 0) {
					checkFile = path + server.cubeName + ".txt";
				}
				else {
					checkFile = path + server.cubeName + "_" + i + ".txt";
				}
				
				log(2, "[" + (i + 1) + "/" + (files) +  "] Importing from: " + checkFile);
				
				
				String[][] ret = server.cube.beginDataload(null, 0, checkFile, IEssOlapFileObject.TYPE_TEXT, false, 0);
				if(ret != null) {
					log(4, "Failed to import data");
					for(String[] arr : ret) {
						for(String item : arr) {
							System.out.println(item);
						}
					}
					return false;
				}
				
				log(2, "Importing file " + checkFile + " complete");
			}
			
			log(2, "Import data for "+ server.getUniqueName() + " complete");
			log(1, "Data imported from " + (files) + " file(s).");
			log(1, server.getUniqueName() + " Lev 0 blocks: " + server.cube.getNonMissingLeafBlocks());
			log(1, server.getUniqueName() + " Parent blocks: " + server.cube.getNonMissingNonLeafBlocks());
			return true;
		}
		catch(EssException e) {
			log(4, "Failed to import data for application: " + server.applicationName);
			log(4, " " + e.getMessage());
			return false;
		}
	}

	public String getApplicationType() {
		try {
			String type =  this.application.getType();
			log(3, "Application Type: " + type);
			return type;
		}
		catch(EssException e) {
			log(4, "Failed to fetch application type. Forcing it to be native(Non-Unicode) application.");
			log(4, e.getMessage());
			return "native";
		}
	}
	
	public boolean isCubeMemberNamesNonUnique() {
		try {
			EssOutlineEditOption outlineEdit = new EssOutlineEditOption();
			outlineEdit.setIncremental(false);
			outlineEdit.setKeepTransaction(false);
			outlineEdit.setLock(false);
			IEssCubeOutline outline = this.cube.openOutline(outlineEdit);
			log(2, "Outline opened to get duplicate member porperties.");
			this.allowDuplicateMembers = outline.isNonUniqueMemberNameEnabled();
			log(3, "Duplicate Members allowed: " + this.allowDuplicateMembers);
			outline.close();
			log(2, "Outline closed");
			return this.allowDuplicateMembers;
		}
		catch(EssException e) {
			log(4, "Failed to fetch Non Unique property from the outline. Force set to disallow duplicate names.");
			return false;
		}
	}
	
	public boolean createSharedPathFolders(boolean create) {
		this.sharedPath = this.sharedPath + this.pathSeperator + this.applicationName + this.pathSeperator + this.cubeName + this.pathSeperator;
		log(3, "Shared path: " + this.sharedPath);
		
		if(create) {
			File path = new File(sharedPath);
			
			if(!path.isDirectory()) {
				log(2, "Path doesn't exist. Trying to create directories.");
				try {
					File mkdir = new File(sharedPath); 
					if(mkdir.mkdirs()) {
						log(1, "Directories created successfully.");
					}
					else {
						log(4, "Could'nt create directories.");
						log(4, "The server on which this java code is running should have read/write access to the path: " + sharedPath);
						return false;
					}
				}
				catch(Exception e) {
					log(4, "Could not access shared path: " + this.sharedPath);
					log(4, " " + e.getMessage());
		        	e.printStackTrace();
		        	return false;
				}
			}
		}
		
		return true;
	}
	
	public void setPathSeperator() {
		if (this.getOS() == 1)
			this.pathSeperator = "\\";
		else if(this.getOS() == 0)
			this.pathSeperator = "/";
		else
			this.pathSeperator = "";
	}
	
	public int getOS() {
		if(this.serverOS.equalsIgnoreCase("WINDOWS")) {
			return 1;
		}
		else if(this.serverOS.equalsIgnoreCase("UNIX")) {
			return 0;
		}
		else {
			return -1;
		}
	}
	
	public String getUniqueName() {
		return "[" + this.providerURL + "].[" + this.serverName + ":" + this.applicationName + ":" + this.cubeName + "] ";
	}
	
	public static void lock(Migration server, int filetype, String filename) {
    	try{
    		server.cube.lockOlapFileObject(filetype, filename);
    	}
    	catch(EssException e){
    		//log(4, "File Lock failed for server: " + server.providerURL + " - " + filetype + " : " + filename);
    		//log(4, e.getMessage());
    		return;
    	}
    }
    
    public static void unlock(Migration server, int filetype, String filename) {
    	try{
    		server.cube.unlockOlapFileObject(filetype, filename);
    	}
    	catch(EssException e){
    		//log(4, "File UNLock failed for server: " + server.providerURL + " - " + filetype + " : " + filename);
    		//log(4, e.getMessage());
    		return;
    	}
    }
    
    // filname seperator "." required
    public static String getFileExtension(int fileType) {
    	switch(fileType){
			case 1:
				return ".otl";
			case 2:
				return ".csc";
			case 4:
				return ".rep";
			case 8:
				return ".rul";
			case 256:
				return ".xls";
			case 2048:
				return ".txt";
			default:
				return "." + fileType;
			/*
			 * TYPE_ALIAS	16
				TYPE_ALL	1073741823
				TYPE_ASCBACKUP	64
				TYPE_BACKUP	192
				TYPE_BINBACKUP	128
				TYPE_CALCSCRIPT	2
				TYPE_DATA	36800
				TYPE_EQD	268435456
				TYPE_EXCEL	256
				TYPE_JAVA_CDF	536870912
				TYPE_LOTUS2	512
				TYPE_LOTUS3	1024
				TYPE_LOTUS4	32768
				TYPE_LRO	134217728
				TYPE_MAX	536870912
				TYPE_OUTLINE	1
				TYPE_PARTITION	1048576
				TYPE_REPORT	4
				TYPE_RULES	8
				TYPE_SELECTION	67108864
				TYPE_STRUCTURE	32
				TYPE_TEXT	2048
				TYPE_WIZARD	65536
				TYPE_WORKSHEET	34560
				TYPE_XML	33280
			 * 
			 */
	}
    }
    
    public static ArrayList<Migration[]> getList() {
    	ArrayList<Migration[]> list = new ArrayList<Migration[]>();
   
    	String csvFile = "MigrationList.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";       
        try {
            br = new BufferedReader(new FileReader(csvFile));
            line = br.readLine(); // Skip Header
            while ((line = br.readLine()) != null) {
                String[] data = line.split(cvsSplitBy);
                
                if(data.length != 14) {
                	log(4, "Invalid CSV format. Force Stop.");
                	System.exit(0);
                }
                
                Migration source = new Migration(data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]);
                Migration destination = new Migration (data[8], data[9], data[10], data[11], null, null, data[12], data[13]);
                
                Migration[] pair = { source, destination };
                list.add(pair);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }
    
	public static void log(int level, String message) {
		String timeStamp = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss").format(Calendar.getInstance().getTime());
		if(level == 1) {
			message = "[INFO]  " + message;
		}
		else if(level == 2) {
			message = "[TRACE] " + message;
		}
		else if(level == 3) {
			message = "[DEBUG] " + message;
		}
		else if(level == 4) {
			message = "[ERROR] " + message;
		}
		System.out.println(timeStamp + message);
	}
	
}
