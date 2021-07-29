package proceso_carga;

public class CSVReaderEventsReact {
	private CSVFileReader reader = new CSVFileReader();
	private Watcher watch = new Watcher(reader.filePath());
	
	public CSVReaderEventsReact() { }
		
	public void observePath() throws InterruptedException {
		int i = -1;
		while(i < 0) {
			reader.readCSV();
			watch.watchService();
		}
	}
	
	public static void main(String[] args) throws InterruptedException {
		CSVReaderEventsReact c = new CSVReaderEventsReact();
		c.observePath();
	}
}
