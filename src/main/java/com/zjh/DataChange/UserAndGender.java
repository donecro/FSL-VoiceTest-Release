package com.zjh.DataChange;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

// 连接users.dat数据和gender.dat数据
public class UserAndGender implements WritableComparable<UserAndGender>{
	private String userID;
	private int gender;
	private int age;
	private String occupation;
	private String zip_code;
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getOccupation() {
		return occupation;
	}
	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}
	public String getZip_code() {
		return zip_code;
	}
	public void setZip_code(String zip_code) {
		this.zip_code = zip_code;
	}
	public UserAndGender() {
		
	}
//	public void UserAndGender(String userID,String gender){
//		this.userID=userID;
//		this.gender=gender;
//	}
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public int getGender() {
		return gender;
	}
	public void setGender(int gender) {
		this.gender = gender;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		this.userID=in.readUTF();
		this.gender=in.readInt();	
		this.age=in.readInt();
		this.occupation=in.readUTF();
		this.zip_code=in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userID);
		out.writeInt(gender);
		out.writeInt(age);
		out.writeUTF(occupation);
		out.writeUTF(zip_code);
	}

	@Override
	public int compareTo(UserAndGender o) {
		int result=this.userID.compareTo(o.userID);
		if(result==0){
			return this.age-o.age;
		}else{
			return result;
		}
	}
	@Override
	public String toString() {
		return this.userID+","+this.gender+","+this.age+","+this.occupation+","+this.zip_code;
	}

}
