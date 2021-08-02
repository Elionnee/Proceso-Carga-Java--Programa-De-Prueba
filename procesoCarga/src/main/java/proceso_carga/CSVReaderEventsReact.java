package proceso_carga;

/**
 * M�todo que actua como main temporal
 * @author snc
 *
 */
public class CSVReaderEventsReact {
	private CSVFileReader reader = new CSVFileReader();
	private Watcher watch = new Watcher(reader.getFilePath());
	
	/**
	 * Constructor de la clase
	 *
	 */
	public CSVReaderEventsReact() { 
		reader = new CSVFileReader();
		watch = new Watcher(reader.getFilePath());
	}
		
	
	
	/**
	 * Metodo que se encarga de observar constantemente el directorio y de leer los archivos que se a�adan 
	 * 
	 * @throws InterruptedException Se lanza cuando se detiene el thread o el observador de forma inesperada
	 */
	public void observePath() throws InterruptedException {
		int i = -1;
		while(i < 0) {
			reader.readCSV();
			watch.watchService();
		}
	}
	
	
	
	/**
	 * Main temporal
	 * 
	 * @throws InterruptedException Se lanza cuando se detiene el thread o el observador de forma inesperada
	 */
	public static void main(String[] args) throws InterruptedException {
		CSVReaderEventsReact c = new CSVReaderEventsReact();
		c.observePath();
	}
}
