package de.fraunhofer.iosb.ilt.sensorthingsimporter.auth;

import de.fraunhofer.iosb.ilt.configurable.Configurable;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;

/**
 *
 * @author scf
 */
public interface AuthMethod extends Configurable<Object, Object> {

	public void setAuth(SensorThingsService service);
}
