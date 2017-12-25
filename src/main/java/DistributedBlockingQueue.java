import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 扩展poll函数，如果消费不到数据，则阻塞，直到能够消费到数据
 * @param <T>
 */
public class DistributedBlockingQueue<T> extends DistributedSimpleQueue<T>{      
    
	
    public DistributedBlockingQueue(ZkClient zkClient, String root) {
    	super(zkClient, root);

	}
    

    @Override
	public T poll() throws Exception {

		while (true){
			
			final CountDownLatch    latch = new CountDownLatch(1);
			final IZkChildListener childListener = new IZkChildListener() {
				
				public void handleChildChange(String parentPath, List<String> currentChilds)
						throws Exception {
					latch.countDown();
					
				}
			};
			zkClient.subscribeChildChanges(root, childListener);
			try{
				T node = super.poll();
	            if ( node != null ){
	                return node;
	            }else{
	            	latch.await();
	            }
			}finally{
				zkClient.unsubscribeChildChanges(root, childListener);
				
			}
			
		}
	}

	
	

}
