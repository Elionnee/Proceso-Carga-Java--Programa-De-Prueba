package proceso_carga;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.hibernate.query.*;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;




interface StringFunction {

	String run(String str);

}



/**
 * TODO PENDIENTE DE SUBDIVIDIR EN SERVICIOS -> LoadService, Watcher...
 * @author snc
 *
 */
public class CSVFileReader {

	private static final String DEFAULTDIR = "C:\\Users\\snc\\Downloads\\miDirDePrueba\\";
	private static String filePath;

	private ArrayList<File> filesRead= new ArrayList<>();
	private Queue<File> filesOnQueue = new LinkedList<>();

	private org.hibernate.SessionFactory sessions;
	private Logger log = null;
	private Properties prop = null;

	private final Semaphore semaforo;

	StringFunction deleteSymbols = (String n) -> {

		String result = ""; 
		result = n.replaceAll("[^a-zA-Z0-9.]", "");  

		return result;

	};



	/**
	 * Constructor de la clase que se encarga de buscar el archivo .properties y de leer que archivos 
	 * csv hay en el directorio que este archivo especifica
	 * 
	 */
	public CSVFileReader() {

		prop = this.loadPropertiesFile("pc.properties");
		log = Logger.getLogger(this.getClass().getName());
		PropertyConfigurator.configure(getClass().getResource("log4j.properties"));

		sessions = new Configuration().configure(new File("src/main/resources/META-INF/hibernate.cfg.xml")).buildSessionFactory();

		log.trace("Logger creado 1");
		log.debug("Logger creado 2");
		log.error("Logger creado 3");
		log.fatal("Logger creado 4");
		log.info("Logger creado 5");
		log.warn("Logger creado 6");

		filesRead.clear();

		setFilePath(this.prop);
		this.semaforo = new Semaphore(Integer.parseInt(prop.getProperty("numThreads")));

	}



	/**
	 * Método que lee los contenidos del archivo .properties
	 * 
	 * @param filePath Ruta en la que se encuentra el archivo pc.properties
	 * 
	 * @return prop Devuelve un objeto del tipo Properties que permite extraer los parametros del archivo .properties
	 */
	public Properties loadPropertiesFile(String filePath) {

		prop = new Properties();

		try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(filePath)) {

			prop.load(resourceAsStream);

		} catch (IOException e) {

			System.err.println("No se pudo leer el archivo .properties : " + filePath);

		}

		return prop;

	}


	/**
	 * Método que se encarga de introducir los datos a la base de datos
	 * 
	 * @param semana Nombre de la tabla a la que se deben añadir los datos
	 * 
	 * @param prod Datos a añadir
	 */
	private synchronized void connectToDBIntroduceData(String semana, ProductoEntity prod) {

		Session session = sessions.openSession();
		semana = semana.replace(".csv", "");


		System.out.println("Conectado satisfactoriamente con la base de datos");
		connectToDBCreateTable(semana, session);

		String query = "INSERT INTO "+ semana + " (Id, Nombre, Precio, Cantidad)\r\n"
				+ "VALUES (?, ?, ?, ?);";

		session.getTransaction().begin();
		session.persist(prod);

		@SuppressWarnings("rawtypes")
		Query query2 = session.createNativeQuery(query);
		query2.setParameter(1, prod.getId());
		query2.setParameter(2, prod.getNombre());
		query2.setParameter(3, Double.toString(prod.getPrecio()));
		query2.setParameter(4, Integer.toString(prod.getCantidad()));

		query2.executeUpdate();
		session.getTransaction().commit();

		session.close();

	}


	/**
	 * Método que crea una nueva tabla en la base de datos
	 * 
	 * @param semana Nombre de la tabla que se debe crear
	 * 
	 * @param session Conexión abierta con la base de datos
	 */
	private void connectToDBCreateTable(String semana, Session session) {

		String queryTable = "CREATE TABLE " + semana + " ("
				+ "    Id text PRIMARY KEY,\n"
				+ "    Nombre text,\n"
				+ "    Precio double,\n"
				+ "    Cantidad int\n"
				+ ");";

		try {

			session.getTransaction().begin();

			@SuppressWarnings("rawtypes")
			Query query = session.createNativeQuery(queryTable);
			query.executeUpdate();

			session.getTransaction().commit();

		} catch (Exception e) {

			System.out.println("Fallo al crear el sessionFactory para comunicarnos con la base de datos");

		}
	}



	/**
	 * Método que devuelve la ruta al directorio que se desea monitorizar
	 * 
	 * @return filePath  Ruta al directorio que se desea monitorizar
	 */
	public String getFilePath() {

		return filePath;

	}




	/**
	 * Método que busca la dirección del archivo .properties
	 * 
	 * @param prop Objeto que contiene los datos del archivo .properties
	 */
	private static void setFilePath(Properties prop) {

		try {

			filePath = prop.getProperty("dir");

		} catch (NullPointerException e) {

			System.out.println("Error al leer el directorio objetivo en el .properties. No se ha encontrado.");

		}

		if(filePath == null) {

			System.out.println(".properties no encontrado, se usara el path absoluto del directorio de pruebas.");
			filePath = DEFAULTDIR; // TODO Borra esto cuando todo funcione que no deberia estar ahí

		}

	}




	/**
	 * Método que se encarga de obtener una lista que contenga todos los csv disponibles 
	 * en el directorio indicado como parametro de entrada.
	 */
	private void getFiles() {

		File folder = new File(filePath);
		File[] filesPresent = folder.listFiles();

		if(filesPresent.length==0) {

			System.out.println("No hay archivos CSV pendientes de leer.");

		} else {

			for(File fileName : filesPresent) { 

				if(fileName.toString().toLowerCase().endsWith(".csv") && (fileName.isFile()) 
						&& (!filesRead.contains(fileName))) {

					filesOnQueue.add(fileName);

				}

			}

		}

	}




	/**
	 * Mueve el archivo indicado al directorio indicado
	 * 
	 * @param orFilePath Nombre del archivo original que se va a mover
	 * 
	 * @param cpFilePath Nombre del archivo una vez movido o del archivo a sustituir
	 * 
	 * @param destDir Directorio al que se va a mover
	 */
	private void moveFile(String orFilePath, String cpFilePath, String destDir) {
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


	/**
	 * Sobreescribe el archivo indicado con el archivo introducido
	 * 
	 * @param orFile Archivo que se desea mover
	 * 
	 * @param cpFile Archivo que se desea sobreescribir
	 * 
	 * @throws IOException Cuando no consigue reemplazar el archivo o cuando no existe
	 */
	private void replaceFile(File orFile, File cpFile) throws IOException {

		try {

			FileUtils.moveFile(orFile, cpFile);

		} catch(FileExistsException e) {

			FileUtils.copyFile(orFile, cpFile);
			FileUtils.forceDelete(orFile);

		}

	}





	/**
	 * Método que se encarga de leer el archivo csv que se le indica como parametro de entrada y que 
	 * almacena los contenidos del mismo en varios hashmap cuyas key son los id de los productos
	 * 
	 * @param file Archivo que se desea leer
	 */
	private void readFile(File file) {

		CSVReader csv = null;
		String semana = file.getName();
		String[] next = null;

		try {

			Reader reader = Files.newBufferedReader(Paths.get(file.getAbsolutePath()));
			System.out.println(file.getAbsolutePath());
			csv = new CSVReaderBuilder(reader).withSkipLines(1).build();

		} catch (IOException e) {

			System.out.println("Error al leer el fichero CSV.");

		}

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





	/**
	 * Recoge todos los archivos nuevos que se encuentran actualmente en el directorio y los lee
	 * 
	 * @throws InterruptedException Se lanza cuando un thread sufre una interrupción inesperada
	 */
	public void readCSV() throws InterruptedException {

		getFiles();
		ExecutorService threadPool = Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("numThreads")));

		while(!filesOnQueue.isEmpty()) {

			threadReadCSVExecution(threadPool);

		}

		try {

			threadPool.awaitTermination(2, TimeUnit.SECONDS);

		} catch (InterruptedException e) {

			Thread.currentThread().interrupt();

		}

	}





	/**
	 * Método que crea los threads y su función de ejecución para leer los archivos CSV
	 * 
	 * @param threadPool Conjunto de threads disponibles
	 * 
	 */
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





	/**
	 * Método que extrae y devuelve el primer archivo de la cola de archivos pendientes por leer
	 * 
	 * @return filesOnQueue.poll() Primer archivo pendiente por leer
	 */
	private synchronized File getFileFromFileQueue() {

		return filesOnQueue.poll();

	}

} 
