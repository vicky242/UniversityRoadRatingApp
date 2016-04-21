package org.abhishaw.roadrate.dao;

public class UserTableConstants {
	public static final String TABLENAME = "user";

	public static class ColumnFamily {
		public static final String PERSONALINFORMATION = "PersonalInformation";
		public static final String RATING = "RoadRating";

		public static class PersonalInformation {
			public static final String ADDRESS = "Address";
			public static final String EMAILID = "EmailId";
			public static final String NAME = "Name";
			public static final String PASSWORD = "Password";
			public static final String PHONENUMBER = "PhoneNumber";
		};
	};
}
