package advanced_java;

import lombok.extern.slf4j.Slf4j;
import org.h2.tools.Server;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.sql.DriverManager.getConnection;

@Slf4j
public class ReflectionPractice {

    static AtomicLong al = new AtomicLong(1000);

    @Retention(RetentionPolicy.RUNTIME)
    public @interface PrimaryKey {
        String name() default "id";
    }

    @Retention(RetentionPolicy.RUNTIME)
    public @interface Column {
        String name();
    }

    /*
        create table Person(
        p_id int primary key,
        p_age int,
        p_name varchar(50)
        )

     */
    static class Person {
        @PrimaryKey(name = "p_id")
        private long id;
        @Column(name = "p_name")
        private String name;
        @Column(name = "p_age")
        private int age;

        public Person() {
        }

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public void setId(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public int getAge() {
            return age;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return "Person{" + "id=" + id + ", name='" + name + '\'' + ", age=" + age + '}';
        }
    }

    static class PrimaryKeyField {
        private final Field field;
        private final PrimaryKey primaryKey;

        public PrimaryKeyField(Field field) {
            this.field = field;
            this.primaryKey = field.getAnnotation(PrimaryKey.class);
        }

        public String getName() {
            return this.primaryKey.name();
        }

        public Class<?> getType() {
            return this.field.getType();
        }

        public Field getField() {
            return field;
        }
    }

    static class ColumnField {
        private final Field field;
        private final Column column;

        public ColumnField(Field field) {
            this.field = field;
            this.column = this.field.getAnnotation(Column.class);
        }

        public String getName() {
            return this.column.name();
        }

        public Class<?> getType() {
            return this.field.getType();
        }

        public Field getField() {
            return field;
        }
    }

    static class MetaModel<T> {

        private final Class<T> clz;

        public MetaModel(Class<T> clz) {
            this.clz = clz;
        }

        public static <T> MetaModel<T> of(Class<T> clz) {
            return new MetaModel<>(clz);
        }

        public Optional<PrimaryKeyField> getPrimaryKey() {
            for (Field declaredField : this.clz.getDeclaredFields()) {
                if (declaredField.isAnnotationPresent(PrimaryKey.class)) {
                    return Optional.of(new PrimaryKeyField(declaredField));
                }
            }
            return Optional.<PrimaryKeyField>empty();
        }

        public List<ColumnField> getColumns() {

            return Arrays.stream(this.clz.getDeclaredFields()).filter(f -> f.isAnnotationPresent(Column.class)).map(ColumnField::new).collect(Collectors.toList());
        }

        public String buildInsertRequest() {
            String primaryKeyColumnName = getPrimaryKey().orElseThrow(() -> new IllegalStateException("Invalid Table without PK")).getName();
            List<String> columnNames = buildColumnNames(primaryKeyColumnName);
            String insertValuePlaceHolders = IntStream.range(0, columnNames.size()).mapToObj(c -> "?").collect(Collectors.joining(", "));
            String insertColumnPlaceHolders = String.join(", ", columnNames);
            return MessageFormat.format("insert into {0} ({1}) values ({2})", this.clz.getSimpleName(), insertColumnPlaceHolders, insertValuePlaceHolders);
        }

        public String buildSelectAllRequest() {
            return MessageFormat.format("select * from {0}", this.clz.getSimpleName());
        }

        public String buildSelectRequest() {
            String primaryKeyColumnName = getPrimaryKey().orElseThrow(() -> new IllegalStateException("Invalid Table without PK")).getName();
            String selectColumnPlaceHolders = String.join(", ", buildColumnNames(primaryKeyColumnName));
            return MessageFormat.format("select {0} from {1} where {2} = ? ", selectColumnPlaceHolders, this.clz.getSimpleName(), primaryKeyColumnName);
        }

        @NotNull
        private List<String> buildColumnNames(String primaryKey) {
            List<String> columnNames = new ArrayList<>();
            columnNames.add(primaryKey);
            columnNames.addAll(getColumns().stream().map(ColumnField::getName).collect(Collectors.toList()));
            return columnNames;
        }
    }

    interface EntityManager<T> {

        void persist(T t) throws SQLException, IllegalAccessException;

        T findById(Object pl) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;

        List<T> findAll() throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;

        void deleteAll() throws SQLException;
    }

    static class H2EntityManager<T> implements EntityManager<T> {

        Class<T> clz;

        public H2EntityManager(Class<T> clz) throws SQLException {
            this.clz = clz;
            deleteAll();
        }

        class PreparedStatementForH2 {
            private PreparedStatement h2Statement;

            public PreparedStatementForH2(PreparedStatement statement) {
                this.h2Statement = statement;
            }

            public PreparedStatement getH2Statement() {
                return h2Statement;
            }

            public PreparedStatement addParams(T t) throws SQLException, IllegalAccessException {
                MetaModel<T> entity = MetaModel.of(clz);
                Class<?> pkType = entity.getPrimaryKey().orElseThrow(() -> new IllegalStateException("Invalid Table without PK")).getField().getType();
                int startIdx = 1;
                if (pkType == long.class) h2Statement.setLong(startIdx++, al.getAndIncrement());

                for (ColumnField column : entity.getColumns()) {
                    Field field = column.getField();
                    Class<?> fieldType = field.getType();
                    field.setAccessible(true);
                    Object value = field.get(t);

                    if (fieldType == int.class) h2Statement.setInt(startIdx++, (int) value);
                    else if (fieldType == String.class) h2Statement.setString(startIdx++, (String) value);
                }
                return h2Statement;
            }

            public PreparedStatement addPrimaryKey(Object primaryKey) throws SQLException {
                if (primaryKey.getClass() == Long.class) {
                    h2Statement.setLong(1, (Long) primaryKey);
                }
                return h2Statement;
            }
        }


        @Override
        public void persist(T t) throws SQLException, IllegalAccessException {

            MetaModel<?> metaModel = MetaModel.of(t.getClass());
            String sqlQuery = metaModel.buildInsertRequest();
            try (PreparedStatement statement = parseSql(sqlQuery).addParams(t)) {
                statement.executeUpdate();
            }

        }

        @Override
        public T findById(Object primaryKey) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
            MetaModel<?> metaModel = MetaModel.of(clz);
            String sqlQuery = metaModel.buildSelectRequest();
            try (PreparedStatement statement = parseSql(sqlQuery).addPrimaryKey(primaryKey); ResultSet resultSet = statement.executeQuery()) {
                List<T> results = buildInstanceFrom(clz, resultSet);
                if (results.isEmpty()) throw new RuntimeException(String.format("ID %s Not Present", primaryKey));
                return results.get(0);
            }
        }

        @Override
        public List<T> findAll() throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
            MetaModel<?> metaModel = MetaModel.of(clz);
            String sqlQuery = metaModel.buildSelectAllRequest();
            try (PreparedStatement statement = parseSql(sqlQuery).getH2Statement(); ResultSet resultSet = statement.executeQuery()) {
                return buildInstanceFrom(clz, resultSet);
            }
        }

        @Override
        public void deleteAll() throws SQLException {
            try (PreparedStatement h2Statement = parseSql("delete " + clz.getSimpleName()).getH2Statement();) {
                log.info("Deleting All Records From {}", clz.getSimpleName());
                h2Statement.execute();
            }
        }

        private List<T> buildInstanceFrom(Class<T> clz, ResultSet resultSet) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, SQLException {
            List<T> results = new ArrayList<>();
            while (resultSet.next()) {
                MetaModel<T> metaModel = MetaModel.of(clz);
                T t = clz.getConstructor().newInstance();
                PrimaryKeyField pkField = metaModel.getPrimaryKey().orElseThrow(() -> new IllegalStateException("Invalid Table without PK"));
                Field field = pkField.getField();
                if (pkField.getType() == long.class) {
                    long pk = resultSet.getInt(pkField.getName());
                    field.setAccessible(true);
                    field.set(t, pk);
                }

                for (ColumnField column : metaModel.getColumns()) {
                    Field columnField = column.getField();
                    columnField.setAccessible(true);
                    if (columnField.getType() == int.class) columnField.set(t, resultSet.getInt(column.getName()));
                    if (columnField.getType() == String.class)
                        columnField.set(t, resultSet.getString(column.getName()));
                }
                results.add(t);
            }

            return results;
        }

        private PreparedStatementForH2 parseSql(String sqlQuery) throws SQLException {
            try (Connection connection = getConnection("jdbc:h2:/Users/thimmv/IdeaProjects/apache-spark-practice/DataEngineeringPractice/db-files/vinodh", "sa", "");
                 PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
                return new PreparedStatementForH2(statement);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Server.main("-ifNotExists");
        log.info("H2 Server Started...");

        EntityManager<Person> em = new H2EntityManager<>(Person.class);

        em.persist(new Person("Vinodh", 30));
        em.persist(new Person("Kumar", 25));
        em.persist(new Person("TVK", 35));

        log.info("{}", em.findAll());
        log.info("{}", em.findById(1001L));
        log.info("{}", em.findById(2001L));


    }


}
