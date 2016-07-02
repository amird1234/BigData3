/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
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
