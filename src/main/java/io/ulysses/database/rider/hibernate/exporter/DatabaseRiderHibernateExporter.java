/**
 * 
 */
package io.ulysses.database.rider.hibernate.exporter;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.criteria.CriteriaBuilder;

import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.PersistenceContext;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ulysses R. Ribeiro
 *
 */
public class DatabaseRiderHibernateExporter {
	
	private static final Logger LOG = LoggerFactory.getLogger(DatabaseRiderHibernateExporter.class);
	
	private final EntityManager entityManager;

	public DatabaseRiderHibernateExporter(EntityManager entityManager) {
		Objects.requireNonNull(entityManager);
		this.entityManager = entityManager;
	}
	
	public EntityManager getEntityManager() {
		return entityManager;
	}
	
	public static DatabaseRiderHibernateExporter createEntityManager(String persistenceUnitName) {		
		EntityManagerFactory factory = Persistence.createEntityManagerFactory(persistenceUnitName);
		return new DatabaseRiderHibernateExporter(factory.createEntityManager());
	}
	
	public CriteriaBuilder getCriteriaBuilder() {
		return this.getEntityManager().getCriteriaBuilder();
	}
	
	public void exportEntityManager(DatabaseRiderWriter writer) throws Exception {
		final SessionImplementor session = this.getEntityManager().unwrap(SessionImplementor.class);
		final PersistenceContext pc = session.getPersistenceContext();
		final Entry<Object,org.hibernate.engine.spi.EntityEntry>[] entityEntries = pc.reentrantSafeEntityEntries();
		LOG.info(String.format("Fetched %d total entries.", entityEntries.length));
		
		LOG.info("Grouping entries for export...");
		Map<Class<?>, Set<Object>> entitiesPerClass = new HashMap<>();
		for (Entry<Object, EntityEntry> entityEntry : entityEntries) {
			Object entity = entityEntry.getKey();
			EntityEntry entityEntryMeta = entityEntry.getValue();
			Class<?> entityClass = entity.getClass();
			
			Set<Object> entities = entitiesPerClass.get(entityClass);
			if (entities == null) {
				if (entityEntryMeta.getEntityKey().getIdentifier() instanceof Comparable) {
					final EntityPersister entityPersister = entityEntryMeta.getPersister();
					final Comparator<Object> entityIdComparator = (e1, e2) -> {
						Comparable<Object> identifier1 = (Comparable<Object>) entityPersister.getIdentifier(e1, session);
						Comparable<Object> identifier2 = (Comparable<Object>) entityPersister.getIdentifier(e2, session);
						
						return identifier1.compareTo(identifier2);
					};
					
					entities = new TreeSet<Object>(entityIdComparator);
				}
				else {
					entities = new HashSet<>();
				}
				entitiesPerClass.put(entityClass, entities);
			}
			entities.add(entity);
			
			LOG.debug("Entity: "+entity+" -> "+entityEntryMeta.getEntityKey().getIdentifier()+" "+entityEntryMeta.getEntityKey().getIdentifier().getClass().getSimpleName());
		}

		writer.write(new MetadataResolver(this.getEntityManager()), entitiesPerClass);
	}
	
	public static void exportEntityManagerToCsv(EntityManager entityManager, String outputPath) {
		exportToCsv(new DatabaseRiderHibernateExporter(entityManager), outputPath);
	}

	public static void exportToCsv(DatabaseRiderHibernateExporter databaseRiderJpaExporter, String outputPath) {
		File outputDir = new File(outputPath);
		outputDir.mkdirs();
		Charset charset = Charset.forName("UTF-8");
		DatabaseRiderCsvWriter databaseRiderCsvWriter = new DatabaseRiderCsvWriter(outputDir, charset);
		
		try {
			databaseRiderJpaExporter.exportEntityManager(databaseRiderCsvWriter);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
