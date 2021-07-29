package proceso_carga;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;

interface StringFunction {
	String run(String str);
}

public class CSVFileReader {

	private static final String DEFAULTDIR = "C:\\Users\\snc\\Downloads\\miDirDePrueba\\";
	private static String filePath;
	private ArrayList<File> filesRead= new ArrayList<>();
	private Queue<File> filesOnQueue = new LinkedList<>();

	private Logger log = null;

	private Properties prop = null;

	private final Semaphore semaforo;

	StringFunction deleteSymbols = (String n) -> {
		String result = ""; 
		result = n.replaceAll("[^a-zA-Z0-9.]", "");  
		return result;
	};


	// Constructor de la clase que se encarga de buscar el archivo .properties y de leer que archivos csv hay en el directorio que este archivo especifica
	public CSVFileReader() {
		prop = this.loadPropertiesFile("pc.properties");
		log = Logger.getLogger(this.getClass());
		PropertyConfigurator.configure(getClass().getResource("log4j.properties"));
		log.trace("Logger creado");
        log.debug("Logger creado");
        log.error("Logger creado");
        log.fatal("Logger creado");
		log.info("Logger creado");
		log.warn("Logger creado");
		filesRead.clear();
		setFilePath(this.prop);
		this.semaforo = new Semaphore(Integer.parseInt(prop.getProperty("numThreads")));
	}



	// Método que lee los contenidos del archivo .properties
	public Properties loadPropertiesFile(String filePath) {
		prop = new Properties();
		try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(filePath)) {
			prop.load(resourceAsStream);
		} catch (IOException e) {
			System.err.println("No se pudo leer el archivo .properties : " + filePath);
		}

		return prop;
	}


	private synchronized void connectToDBIntroduceData(String semana, ProductoEntity prod) {
		EntityManagerFactory entityManagerFactory = Persistence
				.createEntityManagerFactory("CSVFileReader");
		EntityManager entityManager = entityManagerFactory.createEntityManager();
		semana = semana.replace(".csv", "");
		try {
			Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
			System.out.println("Conectado satisfactoriamente con la base de datos");
			connectToDBCreateTable(semana, entityManager);

			String query = "INSERT INTO "+ semana + " (Id, Nombre, Precio, Cantidad)\r\n"
					+ "VALUES (?, ?, ?, ?);";

			entityManager.getTransaction().begin();
			entityManager.persist(prod);
			Query query2 = entityManager.createNativeQuery(query);
			query2.setParameter(1, prod.getId());
			query2.setParameter(2, prod.getNombre());
			query2.setParameter(3, Double.toString(prod.getPrecio()));
			query2.setParameter(4, Integer.toString(prod.getCantidad()));
			query2.executeUpdate();
			entityManager.getTransaction().commit();
		} catch (ClassNotFoundException e) {
			System.out.println("Problema al conectarse a la base de datos. CLASS NOT FOUND");
		} finally {
			entityManager.close();
		}
	}



	private void connectToDBCreateTable(String semana, EntityManager entityManager) {
		String queryTable = "CREATE TABLE " + semana + " ("
				+ "    Id text PRIMARY KEY,\n"
				+ "    Nombre text,\n"
				+ "    Precio double,\n"
				+ "    Cantidad int\n"
				+ ");";
		try {
			entityManager.getTransaction().begin();
			Query query = entityManager.createNativeQuery(queryTable);
			query.executeUpdate();
			entityManager.getTransaction().commit();
		} catch (Exception e) {
			System.out.println("Fallo al crear el sessionFactory para comunicarnos con la base de datos");
		}
	}

	public String filePath() {
		return filePath;
	}


	// Método que busca la dirección del archivo .properties
	private static void setFilePath(Properties prop) {
		try {
			filePath = prop.getProperty("dir");
		} catch (NullPointerException e) {
			System.out.println("Error al leer el directorio objetivo en el .properties. No se ha encontrado.");
		}
		if(filePath == null) {
			System.out.println(".properties no encontrado, se usara el path absoluto del directorio de pruebas.");
			filePath = DEFAULTDIR;
		}
	}


	// Método que se encarga de obtener una lista que contenga todos los csv disponibles 
	// en el directorio indicado como parametro de entrada.
	private void getFiles() {
		File folder = new File(filePath);
		File[] filesPresent = folder.listFiles();
		if(filesPresent.length==0){
			System.out.println("No hay archivos CSV pendientes de leer.");
		}else{
			for(File fileName : filesPresent) { 
				if(fileName.toString().toLowerCase().endsWith(".csv") && (fileName.isFile()) 
						&& (!filesRead.contains(fileName))) {
					filesOnQueue.add(fileName);
				}
			}
		}
	}


	private void moveFile(String orFilePath, String cpFilePath, String destDir)
	{
		try {
			if(StringUtils.isNoneBlank(orFilePath) && StringUtils.isNoneBlank(cpFilePath))
			{
				File orFile = new File(orFilePath);
				File cpFile = new File(cpFilePath);
				replaceFile(orFile, cpFile);
				System.out.println("Archivo trasladado correctramente a la carpeta : " + destDir);

				File destinyDir = new File(destDir);
				FileUtils.moveFileToDirectory(cpFile, destinyDir, true);

			}
		} catch(Exception ex) { 
			ex.printStackTrace();
		}
	}



	private void replaceFile(File orFile, File cpFile) throws IOException {
		try {
			FileUtils.moveFile(orFile, cpFile);
		} catch(FileExistsException e) {
			FileUtils.copyFile(orFile, cpFile);
			FileUtils.forceDelete(orFile);
		}
	}

	// Método que se encarga de leer el archivo csv que se le indica como parametro de entrada y que 
	// almacena los contenidos del mismo en varios hashmap cuyas key son los id de los productos
	private void readFile(File file) {
		CSVReader csv = null;
		String semana = file.getName();

		try {
			Reader reader = Files.newBufferedReader(Paths.get(file.getAbsolutePath()));
			System.out.println(file.getAbsolutePath());
			csv = new CSVReaderBuilder(reader).withSkipLines(1).build();
		} catch (IOException e) {
			System.out.println("Error al leer el fichero CSV.");
		}

		String[] next = null;

		try {
			if(csv == null) {
				throw new NullPointerException();
			}
			while((next = csv.readNext()) != null) {
				connectToDBIntroduceData(semana, new ProductoEntity(next[0], next[1], Double.parseDouble(deleteSymbols.run(next[2])), Integer.parseInt(deleteSymbols.run(next[3]))));
			}
			moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK");
		} catch (CsvValidationException e) {
			System.out.println("CSV es null");
			moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO");
		} catch (IOException e) {
			System.out.println("Error al leer el CSV");
			moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO");
		} catch (NullPointerException e) {
			System.out.println("CSV es nulo");
		}
	}


	public void readCSV() throws InterruptedException {
		getFiles();
		ExecutorService threadPool;
		threadPool = Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("numThreads")));
		while(!filesOnQueue.isEmpty()) {
			threadReadCSVExecution(threadPool);
		}
		try {
			threadPool.awaitTermination(2, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}


	private void threadReadCSVExecution(ExecutorService threadPool) {
		for(int i = 0; i < semaforo.availablePermits(); i++) {
			threadPool.execute(new Runnable() {
				public void run() {
					try {
						semaforo.acquire();
						threadGetFileFromQueue();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					} 
				}

				private void threadGetFileFromQueue() {
					File file;
					try {
						file = getFileFromFileQueue();
						if(file != null) {
							readFile(file);
							filesRead.add(file);
						}
					} catch (NullPointerException e) { 
						e.printStackTrace();
					} finally {
						semaforo.release();
					}
				}
			});
		}
	}
	
	private synchronized File getFileFromFileQueue() {
		return filesOnQueue.poll();
	}

} 
