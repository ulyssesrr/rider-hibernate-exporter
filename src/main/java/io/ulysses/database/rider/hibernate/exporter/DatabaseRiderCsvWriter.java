/**
 * 
 */
package io.ulysses.database.rider.hibernate.exporter;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.univocity.parsers.common.processor.RowWriterProcessor;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

/**
 * @author Ulysses R. Ribeiro
 *
 */
public class DatabaseRiderCsvWriter implements DatabaseRiderWriter {
	
	private static final String TABLE_ORDERING_FILENAME = "table-ordering.txt";
	private final File outputDir;
	private final List<Class<?>> tableOrdering;
	private final Charset charset;

	public DatabaseRiderCsvWriter(File outputDir, Charset charset, List<Class<?>> tableOrdering) {
		Objects.requireNonNull(outputDir, "outputDir cannot be null.");
		
		if (!outputDir.isDirectory()) {
			throw new IllegalArgumentException("Not a directory: "+outputDir);
		}
		this.outputDir = outputDir;
		this.charset = charset;
		this.tableOrdering = tableOrdering;
	}
	
	public DatabaseRiderCsvWriter(File outputDir, Charset charset) {
		this(outputDir, charset, null);
	}
	
	public File getOutputDir() {
		return outputDir;
	}
	
	public Charset getCharset() {
		return charset;
	}
	
	public List<Class<?>> getTableOrdering() {
		return tableOrdering;
	}

	@Override
	public void write(MetadataResolver metadataResolver, Map<Class<?>, Set<Object>> entitiesPerClass) throws Exception {
		// group entities per table
		// more then one entity may use the same table
		LinkedHashMap<String, Map<Class<?>, Set<Object>>> entitiesPerTable = new LinkedHashMap<>();
		for (Entry<Class<?>, Set<Object>> e : entitiesPerClass.entrySet()) {
			Class<?> entityClass = e.getKey();
			Set<Object> entities = e.getValue();
			String tableName = metadataResolver.getTableName(entityClass);
			
			if (!entities.isEmpty()) {
				entitiesPerTable.computeIfAbsent(tableName, k -> new LinkedHashMap<>()).put(entityClass, entities);
			}
		}
		
		for (Entry<String, Map<Class<?>, Set<Object>>> e : entitiesPerTable.entrySet()) {
			String tableName = e.getKey();
			
			Map<Class<?>, Set<Object>> entityPerClassOnTable = e.getValue();
			
			LinkedHashSet<String> headers = entityPerClassOnTable
				.keySet()
				.stream()
				.map(clzz -> metadataResolver.getColumnMapping(clzz))
				.flatMap(colMap -> colMap.keySet().stream())
				.collect(Collectors.toCollection(LinkedHashSet::new));
				
			
			File tableFile = new File(this.getOutputDir(), tableName+".csv");
			FileOutputStream fos = new FileOutputStream(tableFile);
			BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fos, this.getCharset()));
			CsvWriterSettings csvWriterSettings = new CsvWriterSettings();
			csvWriterSettings.setNullValue("null");
			
			csvWriterSettings.setHeaders(headers.stream().toArray(String[]::new));
			
			CsvWriter mainWriter = new CsvWriter(bufferedWriter, csvWriterSettings);
			mainWriter.writeHeaders();
			
			for (Entry<Class<?>, Set<Object>> entry : entityPerClassOnTable.entrySet()) {
				Class<?> entityClass = entry.getKey();
				Set<Object> entities = entry.getValue();
				
				final Map<String, String> columnMapping = metadataResolver.getColumnMapping(entityClass);
				csvWriterSettings.setRowWriterProcessor(new CustomRowWriterProcessor(metadataResolver, columnMapping));
				
				CsvWriter writer = new CsvWriter(bufferedWriter, csvWriterSettings);
				for (Object bean : entities) {
					writer.processRecord(bean);
				}
			}
			mainWriter.close();
		}
		
		List<Class<?>> tableOrder = this.getTableOrdering();
		if (tableOrder == null) {
			tableOrder = new ArrayList<>(entitiesPerClass.keySet());
		}
		Set<String> tables = tableOrder.stream().map(entityClass -> metadataResolver.getTableName(entityClass)).collect(Collectors.toSet());
		
		File tableOrderingFile = new File(this.getOutputDir(), TABLE_ORDERING_FILENAME);
		try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tableOrderingFile), this.getCharset()))) {
			for (String table : tables) {
				bufferedWriter.write(table);
				bufferedWriter.newLine();
			}
		}
	}

	
	
	class CustomRowWriterProcessor implements RowWriterProcessor<Object> {
		
		private MetadataResolver metadataResolver;

		private Map<String, String> columnMapping;


		public CustomRowWriterProcessor(MetadataResolver metadataResolver, Map<String, String> columnMapping) {
			this.metadataResolver = metadataResolver;
			this.columnMapping = columnMapping;
		}
		
		public MetadataResolver getMetadataResolver() {
			return metadataResolver;
		}
		
		public Map<String, String> getColumnMapping() {
			return columnMapping;
		}

		@Override
		public Object[] write(Object input, String[] headers, int[] indexesToWrite) {
			Object[] values = new Object[headers.length];
			for (int i = 0; i < headers.length; i++) {
				String columnName = headers[i];
				String property = this.getColumnMapping().get(columnName);
				
				
				Object value;
				if (i == 0) {
					value = this.getMetadataResolver().getIdValue(input);
				}
				else {
					if (property != null) {
						value = this.getMetadataResolver().getPropertiesValue(input, property, columnName);	
					}
					else {
						value = null;
					}
				}
				values[i] = value;
			}
			return values;
		}
		
	}
}
