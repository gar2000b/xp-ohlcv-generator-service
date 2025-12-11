package ca.digilogue.xp.repository;

import ca.digilogue.xp.model.User;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@Repository
public class UserRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String SELECT_ALL_USERS_SQL =
            "SELECT id, username, first_name, last_name, email FROM users";
    private static final String SELECT_USER_BY_ID_SQL =
            "SELECT id, username, first_name, last_name, email FROM users WHERE id = ?";

    public UserRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<User> findAll() {
        return jdbcTemplate.query(SELECT_ALL_USERS_SQL, new UserRowMapper());
    }

    public User findById(String id) {
        List<User> users = jdbcTemplate.query(SELECT_USER_BY_ID_SQL, new UserRowMapper(), id);
        return users.isEmpty() ? null : users.get(0);
    }

    private static class UserRowMapper implements RowMapper<User> {
        @Override
        public User mapRow(ResultSet rs, int rowNum) throws SQLException {
            User user = new User();
            user.setId(String.valueOf(rs.getLong("id")));
            user.setUsername(rs.getString("username"));
            user.setFirstName(rs.getString("first_name"));
            user.setLastName(rs.getString("last_name"));
            user.setEmail(rs.getString("email"));
            return user;
        }
    }
}
