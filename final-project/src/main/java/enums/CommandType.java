package enums;

/*
Avner Gidron; AvnerGidron1@gmail.com; 201533262
Carmi Arlinsky; 4carmi@gmail.com; 029993904
Samah Ghazawi; idrees.samah@gmail.com; 301416897
Amir dahan; Amird1234@gmail.com; 039593801
*/
public enum CommandType {
	PAGE_RANK("pagerank",1),
	MAP_COMMAND("map",2),
	RECOMMEND("recommend",3),
	COMMANDS("commands",4),
	ILLEGAL("illegal",0);
	
	
	private String stringValue;
	private Integer intValue;
	
	
	CommandType(String name,Integer value){
		setStringValue(name);
		setIntValue(value);
	}


	public String getStringValue() {
		return stringValue;
	}


	public void setStringValue(String stringValue) {
		this.stringValue = stringValue;
	}


	public Integer getIntValue() {
		return intValue;
	}


	public void setIntValue(Integer intValue) {
		this.intValue = intValue;
	}
	
	public static CommandType fromString(String inName){
		switch (inName) {
		case "pagerank":
			return PAGE_RANK;
		case "map":
			return CommandType.MAP_COMMAND;
		case "recommend":
			return CommandType.RECOMMEND;
		case "commands":
			return COMMANDS;
		default:
			return CommandType.ILLEGAL;
		}
	}
}
