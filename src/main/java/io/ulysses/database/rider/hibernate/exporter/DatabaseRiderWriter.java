/**
 * 
 */
package io.ulysses.database.rider.hibernate.exporter;

import java.util.Map;
import java.util.Set;

/**
 * 
 * @author Ulysses R. Ribeiro
 *
 */
public interface DatabaseRiderWriter {

	public void write(MetadataResolver metadataResolver, Map<Class<?>, Set<Object>> entitiesPerClass) throws Exception;
}
