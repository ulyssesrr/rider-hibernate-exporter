/**
 * 
 */
package io.ulysses.database.rider.hibernate.exporter;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import javax.persistence.Table;

import org.apache.commons.beanutils.PropertyUtils;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.persister.entity.AbstractEntityPersister;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.type.Type;

/**
 * @author Ulysses R. Ribeiro
 *
 */
public class MetadataResolver {

	private EntityManager entityManager;

	public MetadataResolver(EntityManager entityManager) {
		this.entityManager = entityManager;
	}
	
	public EntityManager getEntityManager() {
		return entityManager;
	}
	
	public String getTableName(final Class<?> entityClass) {
		Class<?> currentClass = entityClass;
		Table table = null;
		do {
			table = currentClass.getAnnotation(Table.class);
			currentClass = currentClass.getSuperclass();
		} while (table == null && !currentClass.equals(Object.class) && currentClass != null);
		Objects.requireNonNull(table, () -> "No table found for: "+entityClass.getName());
		return table.name();
	}
	
	protected AbstractEntityPersister getClassMetadata(Class<?> entityClass) {
		final SessionImplementor session = this.getEntityManager().unwrap(SessionImplementor.class);
		EntityPersister entityPersister = session.getFactory().locateEntityPersister(entityClass);
		return (AbstractEntityPersister) entityPersister.getClassMetadata();
	}
	
	public String[] getPropertyNames(Class<?> entityClass) {
		AbstractEntityPersister classMetadata = this.getClassMetadata(entityClass);
		
		String[] propertyNames = classMetadata.getPropertyNames();
		String[] allProperties = new String[propertyNames.length + 1];
		allProperties[0] = classMetadata.getIdentifierPropertyName();
		System.arraycopy(propertyNames, 0, allProperties, 1, propertyNames.length);
		return allProperties;
	}
	
	public String[] getPropertyColumnNames(Class<?> entityClass, String propertyName) {
		return this.getClassMetadata(entityClass).getPropertyColumnNames(propertyName);
	}
	
	public Map<String, String> getColumnMapping(final Class<?> entityClass) {
		final Map<String, String> columnMapping = new LinkedHashMap<>();
		for (Entry<String, String[]> propertyCols : this.getPropertiesColumns(entityClass).entrySet()) {
			String propertyName = propertyCols.getKey();
			String[] columns = propertyCols.getValue();
			
			for (String column : columns) {
				columnMapping.put(column, propertyName);
			}
		}
		return columnMapping;
	}
	
	public Object getPropertiesValue(Object entity, String propertyName, String columnName) {
		final SessionImplementor session = this.getEntityManager().unwrap(SessionImplementor.class);
		EntityPersister entityPersister = session.getFactory().locateEntityPersister(entity.getClass());
		AbstractEntityPersister classMetadata = (AbstractEntityPersister) entityPersister.getClassMetadata();
		Type propertyType = classMetadata.getPropertyType(propertyName);
		Object propertyValue = classMetadata.getPropertyValue(entity, propertyName);
//		boolean anyType = propertyType.isAnyType();
		boolean associationType = propertyType.isAssociationType();
//		boolean collectionType = propertyType.isCollectionType();
		boolean componentType = propertyType.isComponentType(); 
//		boolean entityType = propertyType.isEntityType();
		if (associationType) {
			propertyValue = propertyValue != null ? this.getIdValue(propertyType, propertyValue) : propertyValue;
		}
		else if (componentType) {
			propertyValue =  propertyValue != null ? this.getComponentValue(propertyValue, columnName) : propertyValue;
		}
		
		return propertyValue;
	}
	
	private Object getComponentValue(Object propertyValue, String columnName) {
		Objects.requireNonNull(propertyValue);
		Objects.requireNonNull(columnName);
		Class<?> componentClass = propertyValue.getClass();
		Map<String, String> componentColumnMapping = this.getComponentColumnMapping(componentClass);
		String fieldName = componentColumnMapping.get(columnName);
//		Preconditions.checkState(fieldName != null, "Failed to find column: "+columnName+" in "+componentClass.getName());
		if (fieldName == null) {
			return null;
		}
		try {
			return PropertyUtils.getProperty(propertyValue, fieldName);
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new RuntimeException("Failed to find field: "+componentClass.getName()+"."+fieldName, e);
		}
	}
	
	public static List<Field> getAllFields(Class<?> startClass) {
	    List<Field> currentClassFields = new ArrayList<>();
	    Field[] declaredFields = startClass.getDeclaredFields();
	    for (int i = 0; i < declaredFields.length; i++) {
			Field field = declaredFields[i];
			currentClassFields.add(field);
		}
	    
	    Class<?> parentClass = startClass.getSuperclass();

	    if (parentClass != null) {
	        List<Field> parentClassFields = (List<Field>) getAllFields(parentClass);
	        currentClassFields.addAll(parentClassFields);
	    }

	    return currentClassFields;
	}
	
	public Map<String, String> getComponentColumnMapping(final Class<?> entityClass) {
		final Map<String, String> ret = new LinkedHashMap<>();
		List<Field> fields = getAllFields(entityClass);
		for (Field field : fields) {
			Column column = field.getAnnotation(Column.class);
			if (column != null) {
				ret.put(column.name(), field.getName());
			}
		}
		return ret;
	}

	public Map<String, String[]> getPropertiesColumns(final Class<?> entityClass) {
		String[] propertyNames = this.getPropertyNames(entityClass);
		return Arrays.stream(propertyNames)
				.collect(Collectors
				.toMap(
					Function.identity(),
					(propName) -> getPropertyColumnNames(entityClass, propName),
					(u, v) -> {
				        throw new IllegalStateException(String.format("Duplicate key %s", (Object) u));
				    },
					LinkedHashMap::new
				));
	}
	
	public Serializable getIdValue(Type type, Object entity) {
		final Class<?> clazz = type.getReturnedClass();
		return getIdValue(clazz, entity);
	}

	public Serializable getIdValue(final Class<?> clazz, Object entity) {
		return this.getClassMetadata(clazz).getIdentifier(entity);
	}
	
	public Serializable getIdValue(Object entity) {
		return this.getClassMetadata(entity.getClass()).getIdentifier(entity);
	}
}
