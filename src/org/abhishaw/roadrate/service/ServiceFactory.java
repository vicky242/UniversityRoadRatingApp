package org.abhishaw.roadrate.service;

import java.util.HashMap;

public class ServiceFactory {
	private static HashMap<String, ServiceInterface> services = new HashMap<>();

	public static ServiceInterface getService(String serviceType) {
		if (services.containsKey(serviceType) == false)
			services.put(serviceType, createNewServiceClass(serviceType));
		return services.get(serviceType);

	}

	private static ServiceInterface createNewServiceClass(String serviceType) {
		ServiceInterface service = null;
		if ("Login".equals(serviceType))
			service = new LoginService();
		else if ("NewUserAccount".equals(serviceType))
			service = new NewAccountService();
		else if ("RatingRoad".equals(serviceType))
			service = new RateRoadService();
		else if ("UpdateUserInfo".equals(serviceType))
			service = new UpdateUserDetailsService();
		else if ("GetuserDetails".equals(serviceType))
			service = new UserDetailService();
		else if ("GetUserRatedRoads".equals(serviceType))
			service = new UserRatedRoadService();
		return service;
	}
}
