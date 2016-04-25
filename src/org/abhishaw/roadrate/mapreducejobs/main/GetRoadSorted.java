package org.abhishaw.roadrate.mapreducejobs.main;

import java.io.File;
import java.io.IOException;

import org.abhishaw.roadrate.mapreducejobs.road.normalavg.RoadAverageCalculator;
import org.abhishaw.roadrate.mapreducejobs.road.sorterbyavg.RoadSortedToFile;
import org.abhishaw.roadrate.mapreducejobs.road.wtedavgInit.RoadWeightAvgInitializer;
import org.abhishaw.roadrate.mapreducejobs.road.wtedavgcalc.RoadWeightedAverageCalculator;
import org.abhishaw.roadrate.mapreducejobs.user.sortedbywt.UserSortedToAFile;
import org.abhishaw.roadrate.mapreducejobs.user.weightagecalculator.UserWeightageCalculator;
import org.abhishaw.roadrate.mapreducejobs.user.weightageinti.UserWeightInitializer;

public class GetRoadSorted {
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		File file = new File("/tmp/mr/myRoadSummary");
		if (file.exists()) {
			for (String string : file.list()) {
				File content = new File("/tmp/mr/myRoadSummary/" + string);
				content.delete();
			}
			file.delete();
		}
		file = new File("/tmp/mr/myUserSummary");
		if (file.exists()) {
			for (String string : file.list()) {
				File content = new File("/tmp/mr/myUserSummary/" + string);
				content.delete();
			}
			file.delete();
		}
		UserWeightInitializer.main(null);
		RoadAverageCalculator.main(null);
		UserWeightageCalculator.main(null);
		RoadWeightAvgInitializer.main(null);
		RoadWeightedAverageCalculator.main(null);
		RoadSortedToFile.main(null);
		UserSortedToAFile.main(null);
	}
}
