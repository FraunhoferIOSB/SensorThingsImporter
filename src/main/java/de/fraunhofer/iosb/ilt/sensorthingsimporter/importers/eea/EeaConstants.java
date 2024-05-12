/*
 * Copyright (C) 2020 Fraunhofer IOSB
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package de.fraunhofer.iosb.ilt.sensorthingsimporter.importers.eea;

/**
 *
 * @author hylke
 */
public class EeaConstants {

	public static final String TAG_AREA_TYPE = "AirQualityStationArea";
	public static final String TAG_BEGIN_TIME = "beginTime";
	public static final String TAG_COUNTRY_CODE = "countryCode";
	public static final String TAG_END_TIME = "endTime";
	public static final String TAG_LOCAL_ID = "localId";
	public static final String TAG_MEASUREMENT_REGIME = "measurementRegime";
	public static final String TAG_MEDIA_MONITORED = "mediaMonitored";
	public static final String TAG_NETWORK = "AirQualityNetwork";
	public static final String TAG_NETWORK_NAME = "AirQualityNetworkName";
	public static final String TAG_METADATA = "metadata";
	public static final String TAG_MOBILE = "mobile";
	public static final String TAG_NAMESPACE = "namespace";
	public static final String TAG_OWNER = "owner";
	public static final String TAG_RECOMMENDED_UNIT = "recommendedUnit";
	public static final String TAG_SAMPLING_METHOD = "samplingMethod";

	public static final String VALUE_OWNER_EEA = "http://dd.eionet.europa.eu";
	public static final String VALUE_MEDIUM_AIR = "http://inspire.ec.europa.eu/codelist/MediaValue/air";

	private EeaConstants() {
		// Utility class.
	}

}
