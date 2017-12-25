import org.I0Itec.zkclient.ExceptionUtil;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 简单分布式队列
 * @param <T>
 */
public class DistributedSimpleQueue<T> {

	protected final ZkClient zkClient;
	protected final String root; //根节点路径

	protected static final String Node_NAME = "n_";  //子节点名称前缀


	public DistributedSimpleQueue(ZkClient zkClient, String root) {
		this.zkClient = zkClient;
		this.root = root;
	}

    /**
     * 获取队列大小
     * @return
     */
	public int size() {
		return zkClient.getChildren(root).size();
	}

    /**
     * 判断队列是否为空
     * @return
     */
	public boolean isEmpty() {
		return zkClient.getChildren(root).size() == 0;
	}

    /**
     *向队列提交数据
     * @param element  数据内容
     * @return
     * @throws Exception
     */
    public boolean offer(T element) throws Exception{
    	
    	String nodeFullPath = root .concat( "/" ).concat( Node_NAME );
        try {
            zkClient.createPersistentSequential(nodeFullPath , element);  //创建持久顺序节点
        }catch (ZkNoNodeException e) {
        	zkClient.createPersistent(root);
        	offer(element);
        } catch (Exception e) {
            throw ExceptionUtil.convertToRuntimeException(e);
        }
        return true;
    }


    /**
     * 向队列取数据
     * @return
     * @throws Exception
     */
	@SuppressWarnings("unchecked")
	public T poll() throws Exception {
		
		try {

			List<String> list = zkClient.getChildren(root);
			if (list.size() == 0) {
				return null;
			}
            /**
             * 将队列按照由小到大的顺序排序
             */
			Collections.sort(list, new Comparator<String>() {
				public int compare(String lhs, String rhs) {
					return getNodeNumber(lhs, Node_NAME).compareTo(getNodeNumber(rhs, Node_NAME));
				}
			});
			
			for ( String nodeName : list ){
				
				String nodeFullPath = root.concat("/").concat(nodeName);	
				try {
					T node = (T) zkClient.readData(nodeFullPath);
					zkClient.delete(nodeFullPath);
					return node;
				} catch (ZkNoNodeException e) {
					// ignore
				}
			}
			
			return null;
			
		} catch (Exception e) {
			throw ExceptionUtil.convertToRuntimeException(e);
		}

	}

    /**
     * 获取节点编号
     * @param str
     * @param nodeName
     * @return
     */
	private String getNodeNumber(String str, String nodeName) {
		int index = str.lastIndexOf(nodeName);
		if (index >= 0) {
			index += Node_NAME.length();
			return index <= str.length() ? str.substring(index) : "";
		}
		return str;

	}

}
