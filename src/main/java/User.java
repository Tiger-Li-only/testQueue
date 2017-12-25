import java.io.Serializable;

/**
 * 用户实体类
 */
public class User implements Serializable {

	String id;  //用户id
    String name;  //用户姓名
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	
	

}
