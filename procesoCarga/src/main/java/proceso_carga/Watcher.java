package proceso_carga;

import java.util.List;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

public class Watcher {

	File file =  null;
	Path dir = null;
	
	public Watcher(String directory) {
		file =  new File(directory);
		dir = Paths.get(file.getAbsolutePath());
	}

	public void watchService() throws InterruptedException {
		try {
			WatchService watcher = dir.getFileSystem().newWatchService();
			dir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE,
					StandardWatchEventKinds.ENTRY_MODIFY);

			System.out.println("Monitorizando eventos en el directorio...");

			WatchKey watchKey = watcher.take();

			List<WatchEvent<?>> events = watchKey.pollEvents();

			for (WatchEvent<?> event : events) {

				if (event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
					System.out.println("Se ha creado un nuevo archivo o directorio: " + event.context().toString());
				}
				if (event.kind() == StandardWatchEventKinds.ENTRY_DELETE) {
					System.out.println("Se ha borrado un archivo o directorio: " + event.context().toString());
				}
				if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
					System.out.println("Se ha modificado un archivo o directorio: " + event.context().toString());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
			Thread.currentThread().interrupt();
		}
	}
}