package proceso_carga;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.hibernate.query.*;


import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.hibernate.Session;
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

	// Directorio que se desea monitorizar
	private static String filePath;

	// Archivos leidos
	private ArrayList<File> filesRead= new ArrayList<>();
	// Archivos pendientes de leer
	private Queue<File> filesOnQueue = new LinkedList<>();

	// Estado actual del thread, cambia si es interrumpido
	private Boolean threadState = true;



	// Objeto logger que registra el estado del programa por consola
	private org.apache.logging.log4j.Logger logger = null;
	// Objeto properties que nos permite acceder a los contenidos del archivo pc.properties correspondiente
	private Properties prop = null;

	private ArrayList<String> mensajesPend = new ArrayList<>();




	// Lambda que permite dar un formato específico a la string que se le pasa como parámetro de entrada
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

		mensajesPend.clear();

		// Genera la conexión con el archivo .properties indicado
		prop = this.loadPropertiesFile("pc.properties");

		// Crea el logger y lo configura a partir de las indicaciones del fichero log4j2.xml correspondiente
		logger = LogManager.getLogger(this.getClass());
		PropertyConfigurator.configure(getClass().getResource("log4j2.xml"));

		// Comprueba si el logger creado funciona correctamente
		try {
			logger.debug("Logger funciona");
		} catch(Exception e) {
			logger.error("Error con el log", e);
			e.printStackTrace();
		}

		// Limpia las colas de archivos leidos y pendientes de leer
		filesRead.clear();
		filesOnQueue.clear();

		// Setea el directorio indicado en el properties como directorio a observar
		setFilePath(this.prop);

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

		// Crea el objeto .properties
		prop = new Properties();

		// Carga el archivo .properties de su directorio de origen, mediante un path relativo a la carpeta de recursos
		try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(filePath)) {

			// Carga el contenido del fichero .properties en el objeto indicado
			prop.load(resourceAsStream);

		} catch (IOException e) {

			mensajesPend.add("No se pudo leer el archivo .properties : " + filePath);

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

		// Creamos un query que nos permite insertar valores en la base de datos
		String query = "INSERT INTO "+ semana + " (Id, Nombre, Precio, Cantidad, Id_Producto)\r\n"
				+ "VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE Cantidad = VALUES(Cantidad) ;";


		// Inicio de la transacción con la bas
		session.getTransaction().begin();

		// Creamos un objeto query con la string que contiene el query de tipo insert
		@SuppressWarnings("rawtypes")
		Query query2 = session.createNativeQuery(query);

		// Rellenamos los parámetros necesarios para realizar el query de tipo insert
		query2.setParameter(1, prod.getId());
		query2.setParameter(2, prod.getNombre());
		query2.setParameter(3, Double.toString(prod.getPrecio()));
		query2.setParameter(4, Integer.toString(prod.getCantidad()));
		query2.setParameter(5, prod.getTransactionId());

		// Intenta actualizar la base de datos ejecutando la query
		try {
			query2.executeUpdate();
			session.getTransaction().commit();
		} catch(Exception e) {
			session.getTransaction().rollback();
		}
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
	private synchronized void connectToDBCreateTable(String semana, Session session) {

		// Creamos un query del tipo create que nos permitirá crear una tabala con el nombre indicado
		String queryTable = "CREATE TABLE " + semana + " ("
				+ "    Id varchar(80) PRIMARY KEY,\n"
				+ "    Nombre text,\n"
				+ "    Precio double,\n"
				+ "    Cantidad int,\n"
				+ "    Id_Producto text\n"
				+ ");";

		// Comienza la transacción para actualizar la base de datos y crear una tabla usando la query
		try {

			session.getTransaction().begin();

			// Crea el objeto query usando el string que contiene la query del tipo create 
			@SuppressWarnings("rawtypes")
			Query query = session.createNativeQuery(queryTable);
			query.executeUpdate();

			session.getTransaction().commit();

		} catch (Exception e) {

			mensajesPend.add("Fallo al crear la tabla. Ya existe");
			session.getTransaction().rollback();

		}
	}





	private synchronized void connectToDBCreateTableLogs(Session session) {

		// Creamos un query del tipo create que nos permitirá crear una tabala con el nombre indicado
		String queryTable = "CREATE TABLE logs (    Id_Log varchar(80) PRIMARY KEY,    Id_Transacción text,   Info text);";

		// Comienza la transacción para actualizar la base de datos y crear una tabla usando la query
		try {

			session.getTransaction().begin();

			// Crea el objeto query usando el string que contiene la query del tipo create 
			@SuppressWarnings("rawtypes")
			Query query = session.createNativeQuery(queryTable);
			query.executeUpdate();

			session.getTransaction().commit();

		} catch (Exception e) {

			mensajesPend.add("Fallo al crear la tabla de logs, ya existe.");
			session.getTransaction().rollback();

		}
	}








	private synchronized void connectToDBIntroduceLogs(Session session, String semana, ProductoEntity prod, String info) {

		// Creamos un query que nos permite insertar valores en la base de datos
		String query = "INSERT INTO  logs (Id_Log, Id_Transacción, Info)\r\n"
				+ "VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE Info = VALUES(Info) ;";


		// Inicio de la transacción con la bas
		session.getTransaction().begin();

		// Creamos un objeto query con la string que contiene el query de tipo insert
		@SuppressWarnings("rawtypes")
		Query query2 = session.createNativeQuery(query);

		// Rellenamos los parámetros necesarios para realizar el query de tipo insert
		query2.setParameter(1, UUID.randomUUID().toString());
		if(prod != null) {
			query2.setParameter(2, prod.getId() + "_" + semana);
		} else {
			query2.setParameter(2, "INFO_GENERAL_" + semana);
		}
		query2.setParameter(3, info);

		// Intenta actualizar la base de datos ejecutando la query
		try {
			query2.executeUpdate();
			session.getTransaction().commit();
		} catch(Exception e) {
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
	private void setFilePath(Properties prop) {

		// Extraemos el filepath del directorio que se desea monitorizar del fichero .properties
		try {

			filePath = prop.getProperty("dir");

		} catch (NullPointerException e) {

			mensajesPend.add("Error al leer el directorio objetivo en el .properties. No se ha encontrado.");

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

		// Obtiene la referencia al directorio y extrae todos los archivos presentes en el mismo
		File folder = new File(filePath);
		File[] filesPresent = folder.listFiles();

		// Comprueba que en verdad cintiene archivos
		if(filesPresent.length==0) {

			mensajesPend.add(Thread.currentThread().getId() + " : No hay archivos CSV pendientes de leer.");

		} else {

			// Comprueba que los archivos presentes son .CSV y que no habian sido leidos previamente durante esta ejecución
			for(File fileName : filesPresent) { 

				if(fileName.toString().toLowerCase().endsWith(".csv") && (fileName.isFile()) 
						&& (!filesRead.contains(fileName))) {

					filesOnQueue.add(fileName);

				} else {

					mensajesPend.add(Thread.currentThread().getName() + " : No hay archivos CSV pendientes de leer.");

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
				mensajesPend.add("Archivo trasladado correctramente a la carpeta : " + destDir);

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
		ProductoEntity p = null;
		// Crea un reader para leer el archivo .csv que le pasan como parametro de entrada, saltandose la 1º línea
		try {

			Reader reader = Files.newBufferedReader(Paths.get(file.getAbsolutePath()));
			csv = new CSVReaderBuilder(reader).withSkipLines(1).build();

		} catch (IOException e) {

			connectToDBIntroduceLogs(session, "Error", null , "Error al leer el fichero CSV.");

		}


		if(csv != null) {

			semana = semana.replace(".csv", "");


			connectToDBIntroduceLogs(session, semana, null, "Conectado satisfactoriamente con la base de datos");

			connectToDBCreateTable(semana, session);
			connectToDBCreateTable("productoentity", session);


			try {

				// Mientras haya una línea de CSV por leer, continua
				while((next = csv.readNext()) != null) {

					// Crea una entidad nueva (Un producto nuevo)
					p = new ProductoEntity(next[0] + "_" + semana, next[1], Double.parseDouble(deleteSymbols.run(next[2])), Integer.parseInt(deleteSymbols.run(next[3])), next[0]);

					// Introduce la nueva entidad en su tabla correspondiente y en la tabla general
					connectToDBIntroduceData(session, semana, p);
					connectToDBIntroduceData(session, "productoentity", p);

				}

				// En caso de salir bien, mueve el archivo a la 
				moveFile(file.getAbsolutePath(), (prop.getProperty("ok") + "\\" + file.getName()), prop.getProperty("ok"));

			} catch (CsvValidationException e) {

				connectToDBIntroduceLogs(session, semana, p, "CSV es null");
				moveFile(file.getAbsolutePath(), prop.getProperty("ko") + "\\" + file.getName(), prop.getProperty("ko"));

			} catch (IOException e) {

				connectToDBIntroduceLogs(session, semana, p, "Error al leer el CSV");
				moveFile(file.getAbsolutePath(), prop.getProperty("ko") + "\\" + file.getName(), prop.getProperty("ko"));

			} catch (NullPointerException e) {

				connectToDBIntroduceLogs(session, semana, p, "CSV es nulo");
				moveFile(file.getAbsolutePath(), prop.getProperty("ko") + "\\" + file.getName(), prop.getProperty("ko"));

			}
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
	public void readCSV(Session session) {

		Boolean pendiente = false;

		ArrayList<String> tempPend;

		tempPend = mensajesPend;
		mensajesPend.clear();
		for (String m : tempPend) {
			connectToDBIntroduceLogs(session, "Inicio", null,  m);
		}
		tempPend.clear();

		connectToDBCreateTableLogs(session);

		connectToDBIntroduceLogs(session, "Inicio", null, "Comienzo de transferencia de archivos .CSV a la base de datos.");

		getFiles();

		if (!filesOnQueue.isEmpty()) {

			ExecutorService threadPool = Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("numThreads")));
			pendiente = true;

			while(Boolean.TRUE.equals(pendiente)) {

				pendiente = threadReadCSVExecution(session, threadPool);

			}
		}


		tempPend = mensajesPend;
		mensajesPend.clear();
		for (String m : tempPend) {
			connectToDBIntroduceLogs(session, "Fin", null,  m);
		}

		tempPend.clear();


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
	private Boolean threadReadCSVExecution(final Session session, ExecutorService threadPool) {

		threadPool.execute(new Runnable() {

			public void run() {

				try {
					threadGetFileFromQueue();
				} catch (Exception e) {
					notifyThreadInt();
				}

			}

			private void threadGetFileFromQueue() {

				File file;

				file = getFileFromFileQueue();

				if(file != null) {

					readFile(session, file);
					filesRead.add(file);
					setThreadState();
				} else {
					notifyThreadInt();
					Thread.currentThread().interrupt();
				}

			}

		});

		if(Boolean.FALSE.equals(threadState)) {
			setThreadState();
			return false;
		} else {
			return true;
		}

	}













	private synchronized void notifyThreadInt() {
		threadState = false;
	}












	private synchronized void setThreadState() {
		threadState = true;
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
