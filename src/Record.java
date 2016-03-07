
public class Record {
	private String roadId;
	private Integer totalRating;
	private Integer users;
	public Record(String roadId, String rating) {
		this.roadId = roadId;
		totalRating = Integer.parseInt(rating);
		users = 1;
		
	}
	public Record() {
		super();
	}
	public String getRoadId() {
		return roadId;
	}
	public void setRoadId(String roadId) {
		this.roadId = roadId;
	}
	public Integer getRating() {
		return totalRating;
	}
	public void setRating(String rating) {
		this.totalRating = Integer.parseInt(rating);
	}
	public Integer getUserInput() {
		return users;
	}
	public void setUsers(String userInput) {
		this.users = Integer.parseInt(userInput);
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return "[" + roadId+", " + totalRating+", " + users+"]";
	}
	
	
}
