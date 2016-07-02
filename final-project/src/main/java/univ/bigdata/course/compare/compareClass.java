package univ.bigdata.course.compare;

import scala.Serializable;

public class compareClass  implements Serializable,Comparable<compareClass>{
	private long firstKey;
	private String secoundKey;
	
	public compareClass(long _firstKey, String _secoundKey){
		this.firstKey=_firstKey;
		this.secoundKey = _secoundKey;
	}
	public long getFirstKey(){
		return firstKey;
	}
	public String getSecoundKey(){
		return secoundKey;
	}
    @Override
    public int compareTo(compareClass other){
        long diff = this.getFirstKey()- other.getFirstKey();
        int res;
        if (diff > 0){
        	return 1;
        }else {
        	if (diff < 0){
        		return -1;
        	}else{
        		return -this.getSecoundKey().compareTo(other.getSecoundKey());
        	}
        }
    }


}
