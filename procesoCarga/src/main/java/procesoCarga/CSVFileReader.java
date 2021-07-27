package procesoCarga;

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

import java.sql.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.commons.io.FileExistsException;
import org.apache.commons.io.FileUtils;

interface StringFunction {
	String run(String str);
}

public class CSVFileReader {

	// private String databaseURL = "jdbc:ucanaccess://C:/Users/snc/Desktop/server/Database2.accdb";

	private static String FILE_PATH;
	private ArrayList<File> filesRead= new ArrayList<File>();
	private Queue<File> filesOnQueue = new LinkedList<File>();
	
	private Logger log = null;

	public Properties prop = null;

	private ExecutorService threadPool;
	private final Semaphore semaforo;

	StringFunction deleteSymbols = new StringFunction() {
		public String run(String n) {
			String result = ""; 
			result = n.replaceAll("[^a-zA-Z0-9.]", "");  
			return result;}
	};


	// Constructor de la clase que se encarga de buscar el archivo .properties y de leer que archivos csv hay en el directorio que este archivo especifica
	public CSVFileReader() {
		log = Logger.getLogger(this.getClass());
		BasicConfigurator.configure();
		log.info("Logger creado");
		filesRead.clear();
		prop = this.loadPropertiesFile("pc.properties");
		setFilePath();
		this.semaforo = new Semaphore(Integer.parseInt(prop.getProperty("numThreads")));
		// this.semaforo = new Semaphore(2);
	}

	

	// Método que lee los contenidos del archivo .properties
	public Properties loadPropertiesFile(String filePath) {
        Properties prop = new Properties();

        try (InputStream resourceAsStream = getClass().getClassLoader().getResourceAsStream(filePath)) {
            prop.load(resourceAsStream);
        } catch (IOException e) {
            System.err.println("No se pudo leer el archivo .properties : " + filePath);
        }

        return prop;
    }
	
	
	private synchronized void ConnectToDB_IntroduceData(String semana, ProductoEntity prod) throws SQLException {
		// Connection con = null;
		EntityManagerFactory entityManagerFactory = Persistence
                .createEntityManagerFactory("CSVFileReader");
        EntityManager entityManager = entityManagerFactory.createEntityManager();
		semana = semana.replace(".csv", "");
		try {
			Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
			// con = DriverManager.getConnection(databaseURL);
			System.out.println("Conectado satisfactoriamente con la base de datos");
			String queryTable = "CREATE TABLE " + semana + " ("
					+ "    Id text PRIMARY KEY,\n"
					+ "    Nombre text,\n"
					+ "    Precio double,\n"
					+ "    Cantidad int\n"
					+ ");";
			try {
				// Statement st=con.createStatement();
				// int tablaCreada = st.executeUpdate(queryTable);
				// System.out.println("Tabla Creada : " + tablaCreada);
				entityManager.getTransaction().begin();
				Query query = entityManager.createNativeQuery(queryTable);
				query.executeUpdate();
				entityManager.getTransaction().commit();
			/** } catch (SQLException ex) {
				System.out.println("Tabla ya existe"); **/
			} catch (Throwable e) {
				System.out.println("Fallo al crear el sessionFactory para comunicarnos con la base de datos");
			}

			String query = "INSERT INTO "+ semana + " (Id, Nombre, Precio, Cantidad)\r\n"
					+ "VALUES (?, ?, ?, ?);";
			
			/** PreparedStatement st=con.prepareStatement(query);
			st.setString(1, Id);
			st.setString(2, nombre);
			st.setString(3, Double.toString(precio));
			st.setString(4, Integer.toString(cantidad));
			int filaActualizada = st.executeUpdate();
			System.out.println("Fila Actualizada : " + filaActualizada); **/
			
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
			/**try {
				if(con != null) {
					con.close();
				}
			} catch (SQLException e) {
				System.out.println("Problema al cerrar la conexión con la base de datos");
			} **/
		}
	}

	public String filePath() {
		return FILE_PATH;
	}


	// Método que busca la dirección del archivo .properties
	private void setFilePath() {
		try {
			FILE_PATH = prop.getProperty("dir");
		} catch (NullPointerException e) {
			System.out.println("Error al leer el directorio objetivo en el .properties. No se ha encontrado.");
		}
		if(FILE_PATH == null) {
			System.out.println(".properties no encontrado, se usara el path absoluto del directorio de pruebas.");
			FILE_PATH = "C:\\Users\\snc\\Downloads\\miDirDePrueba\\";
		}
	}
	

	// Método que se encarga de obtener una lista que contenga todos los csv disponibles 
	// en el directorio indicado como parametro de entrada.
	private void getFiles() {
		File folder = new File(FILE_PATH);
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
				try {
					FileUtils.moveFile(orFile, cpFile);
				} catch(FileExistsException e) {
					FileUtils.copyFile(orFile, cpFile);
					FileUtils.forceDelete(orFile);
				}
					System.out.println("Archivo trasladado correctramente a la carpeta : " + destDir);

					File destinyDir = new File(destDir);
					FileUtils.moveFileToDirectory(cpFile, destinyDir, true);

				}
			} catch(Exception ex) {}
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
				while((next = csv.readNext()) != null) {
					ConnectToDB_IntroduceData(semana, new ProductoEntity(next[0], next[1], Double.parseDouble(deleteSymbols.run(next[2])), Integer.parseInt(deleteSymbols.run(next[3]))));
				}
				moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK");
			} catch (SQLException e) {
				moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\OK");
				System.out.println("Entrada previamente introducida en la base de datos.");
			} catch (CsvValidationException e) {
				System.out.println("CSV es null");
				moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO");
			} catch (IOException e) {
				System.out.println("Error al leer el CSV");
				moveFile(file.getAbsolutePath(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO\\" + file.getName(), "C:\\Users\\snc\\Downloads\\miDirDePrueba\\KO");
			}
		}


		public void readCSV() {
			getFiles();
			// threadPool = Executors.newFixedThreadPool(Integer.parseInt(prop.getProperty("numThreads")));
			threadPool = Executors.newFixedThreadPool(2);
			while(!filesOnQueue.isEmpty()) {
				for(int i = 0; i < semaforo.availablePermits(); i++) {
					threadPool.execute(new Runnable() {
						public void run() {
							File file = null;
							try {
								semaforo.acquire();
								try {
									file = filesOnQueue.poll();
									readFile(file);
									filesRead.add(file);
								} catch (NullPointerException e) { 
								} finally {
									semaforo.release();
								}
							} catch (InterruptedException e) {
								Thread.currentThread().interrupt();
							} 
							return;}
					});
				}
			}
			try {
				threadPool.awaitTermination(2, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			filesOnQueue.clear();
		}

	} 
