package procesoCarga;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
public class ProductoEntity  {
	@Id
	private String id;
	private String nombre; 
	private Double precio;   
	private int cantidad;  

	public ProductoEntity () {}

	public ProductoEntity (String id, String nombre, Double precio, int cantidad) {
		super();
		this.setId(id);
		this.setNombre(nombre);
		this.setPrecio(precio);
		this.setCantidad(cantidad);
	}

	public String getId() {
		return id;
	}

	public void setId( String id ) {
		this.id = id;
	}

	public String getNombre() {
		return nombre;
	}

	public void setNombre(String nombre) {
		this.nombre = nombre;
	}

	public Double getPrecio() {
		return precio;
	}

	public void setPrecio(Double precio) {
		this.precio = precio;
	}

	public int getCantidad() {
		return cantidad;
	}

	public void setCantidad(int cantidad) {
		this.cantidad = cantidad;
	}

}
