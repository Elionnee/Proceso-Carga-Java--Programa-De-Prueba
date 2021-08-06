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

	private ArrayList<File> filesRead= new ArrayList<File>();
	private Queue<File> filesOnQueue = new LinkedList<File>();
	
	private org.hibernate.SessionFactory sessions;
	private Logger log = null;
	private Properties prop = null;

	private final Semaphore semaforo;

	StringFunction deleteSymbols = new StringFunction() {
		@Override
		public String run(String n) {

			String result = ""; 
			result = n.replaceAll("[^a-zA-Z0-9.]", "");  

			return result;

		}
	};



	/**
	 * Constructor de la clase que se encarga de buscar el archivo .properties y de leer que archivos 
	 * csv hay en el directorio que este archivo especifica
	 * 
	 */
	public CSVFileReader() {

		prop = this.loadPropertiesFile("pc.properties");
		
		log = Logger.getLogger(this.getClass().getName());
		PropertyConfigurator.configure(getClass().getResource("log4j2.xml"));
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
	 * Crea un objeto 'properties'
	 * Busca el archivo .properties en el directorio indicado y trata de leerlo. En caso de no poder, lo notifica
	 * Guarda la referencia al contenido del archivo en el objeto 'properties'
	 * Devuelve el objeto 'properties'
	 */

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
	 * Conectarse a la base de datos
	 * Tratar de crear una tabla con el nombre. En caso de que ya exista, se notifica 
	 * Se crea una query con los datos del producto
	 * Se ejecuta la query
	 * Se cierra la conexión con la base de datos
	 */

	/**
	 * Método que se encarga de introducir los datos a la base de datos
	 * 
	 * @param semana Nombre de la tabla a la que se deben añadir los datos
	 * 
	 * @param prod Datos a añadir
	 */
	private synchronized void connectToDBIntroduceData(Session session, String semana, ProductoEntity prod) {
		
		String query = "INSERT INTO "+ semana + " (Id, Nombre, Precio, Cantidad, Id_Producto)\r\n"
				+ "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE Cantidad = VALUES(Cantidad) ;";

		if(session.getTransaction().isActive()) {
			session.getTransaction().rollback();
		}
		
		session.getTransaction().begin();
		session.persist(prod);

		@SuppressWarnings("rawtypes")
		Query query2 = session.createNativeQuery(query);
		
		query2.setParameter(1, prod.getId());
		query2.setParameter(2, prod.getNombre());
		query2.setParameter(3, Double.toString(prod.getPrecio()));
		query2.setParameter(4, Integer.toString(prod.getCantidad()));
		query2.setParameter(5, prod.getTransactionId());

		query2.executeUpdate();
		session.getTransaction().commit();
	}

	
	/**
	 * Se crea una query para crear la tabla con el nombre indicado, utilizando la conexión ya establecida con la base de datos
	 * Se ejecuta la query
	 */

	/**
	 * Método que crea una nueva tabla en la base de datos
	 * 
	 * @param semana Nombre de la tabla que se debe crear
	 * 
	 * @param session Conexión abierta con la base de datos
	 */
	private void connectToDBCreateTable(String semana, Session session) {

		String queryTable = "CREATE TABLE " + semana + " ("
				+ "    Id varchar(80) PRIMARY KEY,\n"
				+ "    Nombre text,\n"
				+ "    Precio double,\n"
				+ "    Cantidad int,\n"
				+ "    Id_Producto text\n"
				+ ");";
		
		if(session.getTransaction().isActive()) {
			session.getTransaction().rollback();
		}

		try {
			
			session.getTransaction().begin();

			@SuppressWarnings("rawtypes")
			Query query = session.createNativeQuery(queryTable);
			query.executeUpdate();

			session.getTransaction().commit();

		} catch (Exception e) {

			System.out.println("Fallo al crear la tabla. Ya existe");
			session.getTransaction().rollback();

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
	 * Buscamos el valor de la etiqueta 'dir' en la referencia al archivo .properties
	 * En caso de haber un error, notificamos del mismo
	 */

	/**
	 * Método que busca la dirección del directorio a monitorizar del archivo .properties
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
	 * Buscar el directorio a monitorizar
	 * Obtener una lista con todos los archivos que contiene en ese momento
	 * Guardar en una lista solo aquellos archivos con extensión .CSV en una lista de 'pendientes por procesar'
	 */

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
	 * Comprobar que los archivos introducidos y el directorio de destino no son nulos
	 * Tratar de mover el archivo al directorio destino
	 * En caso de que el archivo ya exista, reemplazar el mismo con el archivo nuevo y borrar este último de su localización anterior
	 * Notificar de cualquier error
	 */

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
			
			if(StringUtils.isNoneBlank(orFilePath) && StringUtils.isNoneBlank(destDir) && StringUtils.isNoneBlank(destDir)) {
				
				File orFile = new File(orFilePath);
				File cpFile = new File(cpFilePath);

				replaceFile(orFile, cpFile);
				System.out.println("Archivo trasladado correctramente a la carpeta : " + destDir);

			}
			
		} catch(Exception ex) { 
			
			ex.printStackTrace();
		
		}
		
	}


	/**
	 * Mover el archivo indicado al directorio indicado
	 * En caso de no poder, sobreescribir el archivo ya existente con el mismo nombre
	 * Borrar el archivo de su directorio original
	 */
	
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
	 * Crear un lector para archivos CSV que lea el archivo indicado como parametro de entrada
	 * Saltar la primera línea del archivo, ya que no contiene datos
	 * Comenzar a leer el archivo línea por línea
	 * Añadir el archivo a la base de datos línea por línea
	 * Si se añade correctamente -> Mover a la carpeta OK
	 * Si no -> Mover a la carpeta KO
	 */

	/**
	 * Método que se encarga de leer el archivo csv que se le indica como parametro de entrada y que 
	 * almacena los contenidos del mismo en varios hashmap cuyas key son los id de los productos
	 * 
	 * @param file Archivo que se desea leer
	 */
	private void readFile(Session session, File file) {

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

			ProductoEntity p = null;
			semana = semana.replace(".csv", "");
			
			while((next = csv.readNext()) != null) {
				
				if (p == null) {
					
					System.out.println("Conectado satisfactoriamente con la base de datos");
					
					connectToDBCreateTable(semana, session);
					connectToDBCreateTable("productoentity", session);
					
				}
				
				p = new ProductoEntity(next[0] + "_" + semana, next[1], Double.parseDouble(deleteSymbols.run(next[2])), Integer.parseInt(deleteSymbols.run(next[3])), next[0]);
				
				connectToDBIntroduceData(session, semana, p);
				connectToDBIntroduceData(session, "productoentity", p);

			}
			
			moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK");

		} catch (CsvValidationException e) {

			System.out.println("CSV es null");
			moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO");

		} catch (IOException e) {

			System.out.println("Error al leer el CSV");
			moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO");

		} catch (NullPointerException e) {
			
			e.printStackTrace();
			System.out.println("CSV es nulo");
			moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO");

		}

	}


	
	/**
	 * Obtener la lista de archivos pendientes de leer
	 * Mientras queden archivos pendientes por leer, asignar a los threads disponibles un archivo a leer
	 * Interrumpir el thread en caso de error y notificar del mismo
	 */

	/**
	 * Recoge todos los archivos nuevos que se encuentran actualmente en el directorio y los lee
	 * 
	 * @throws InterruptedException Se lanza cuando un thread sufre una interrupción inesperada
	 */
	public void readCSV() throws InterruptedException {

		getFiles();
		ExecutorService threadPool = Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("numThreads")));
		
		Session session = sessions.openSession();
		
		while(!filesOnQueue.isEmpty()) {

			threadReadCSVExecution(session, threadPool);

		}
		
		try {

			threadPool.awaitTermination(2, TimeUnit.SECONDS);

		} catch (InterruptedException e) {

			Thread.currentThread().interrupt();
			System.out.println("Thread interrumpido");

		}
		
		session.close();

	}



	/**
	 * Por cada thread disponible, comenzar a ejecutar la funciópn run() :
	 * 		Adquirir un semaforo
	 * 		Obtener archivo de la cola de pendientes
	 * 		Avisar en caso de que haya una interrupción inesperada y detener el thread
	 * 
	 */

	/**
	 * Método que crea los threads y su función de ejecución para leer los archivos CSV
	 * 
	 * @param threadPool Conjunto de threads disponibles
	 * 
	 */
	private void threadReadCSVExecution(final Session session, ExecutorService threadPool) {

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

							readFile(session, file);
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
		if(!filesOnQueue.isEmpty()) {
			return filesOnQueue.poll();
		}
		return null;
	}

} 
