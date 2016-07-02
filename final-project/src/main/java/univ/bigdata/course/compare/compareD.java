package univ.bigdata.course.compare;

import scala.Serializable;

public class compareD implements Serializable,Comparable<compareD>{
	private Double firstKey;
	private String secoundKey;
	
	public compareD(Double _firstKey, String _secoundKey){
		this.firstKey=_firstKey;
		this.secoundKey = _secoundKey;
	}
	public double getFirstKey(){
		return firstKey;
	}
	public String getSecoundKey(){
		return secoundKey;
	}
    public int compareTo(compareD other){
        Double diff = this.getFirstKey()- other.getFirstKey();
        int res;
        if (diff > 0){
        	return 1;
        }else {
        	if (diff < 0){
        		return -1;
        	}
        }
        String s1 = this.getSecoundKey();
        String s2 = other.getSecoundKey();
        for (int i = 0; i< s1.length();++i){
        	if(s1.charAt(i)>s2.charAt(i)){
        		return -1;
        	}
        	if(s1.charAt(i)<s2.charAt(i)){
        		return 1;
        	}
        }
        return 0;
        //return res == 0 ? this.getSecoundKey().compareTo(other.getSecoundKey()) : res;
    	
    }

}
