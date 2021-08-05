package proceso_carga;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;


/**
 * Clase que representa cada una de las entradas de una tabla de la base de datos.
 * @author snc
 *
 */
@Entity
public class ProductoEntity  {
	
	@Id
    @Column(name = "Id")
	private String id;
	@Column(name = "Nombre")
	private String nombre; 
	@Column(name = "Precio")
	private Double precio; 
	@Column(name = "Cantidad")
	private int cantidad;  

	
	
	
	/**
	 *  Constructor vac�o de la clase, en caso de que el archivo contenga entradas vac�as
	 * 
	 */
	public ProductoEntity () {}
	
	
	
	
	
	/**
	 *  Contructor de la clase
	 * 
	 *  @param id Identificador del producto
	 *  
	 *  @param nombre Nombre del producto
	 *  
	 *  @param precio Precio por unidad del producto
	 *  
	 *  @param cantidad Cantidad del producto adquirida
	 */
	public ProductoEntity (String id, String nombre, Double precio, int cantidad) {
		
		super();
		this.setId(id);
		this.setNombre(nombre);
		this.setPrecio(precio);
		this.setCantidad(cantidad);
		
	}

	
	/**
	 * M�todo que retorna el id del producto
	 * 
	 * @return id Identificador del producto
	 */
	public String getId() {
		
		return id;
		
	}
	
	
	
	/**
	 * M�todo que da valor al identificador del producto
	 * 
	 * @param id Valor deseado para el identificador del producto
	 */
	public void setId( String id ) {
		
		this.id = id;
		
	}

	
	/**
	 * M�todo que retorna el nombre del producto
	 * 
	 * @return nombre Nombre del producto
	 */
	public String getNombre() {
		
		return nombre;
		
	}
	
	
	
	/**
	 * M�todo que da valor al nombre del producto
	 * 
	 * @param nombre Nombre que se desea dar al producto
	 */
	public void setNombre(String nombre) {
		
		this.nombre = nombre;
		
	}
	
	
	
	/**
	 * M�todo que retorna el precio del producto
	 * 
	 * @return precio Precio del producto
	 */
	public Double getPrecio() {
		
		return precio;
		
	}

	
	
	/**
	 * M�todo que da valor al precio del producto 
	 * 
	 * @param precio Precio que se desea dar al producto
	 */
	public void setPrecio(Double precio) {
		
		this.precio = precio;
		
	}

	
	
	/**
	 * M�todo que retorna la cantidad que se ha encargado del producto
	 * 
	 * @return Cantidad encargada del producto
	 */
	public int getCantidad() {
		
		return cantidad;
		
	}

	
	
	
	/**
	 * M�todo que da valor a la cantidad de producto que se ha encargado
	 * 
	 * @param cantidad Cantidad del producto encargada
	 */
	public void setCantidad(int cantidad) {
		
		this.cantidad = cantidad;
		
	}

}
