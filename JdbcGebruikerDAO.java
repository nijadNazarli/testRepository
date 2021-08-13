package com.example.springjdbctemplate.Repository;

import com.example.springjdbctemplate.Gebruiker.Gebruiker;
import com.example.springjdbctemplate.Gebruiker.GebruikerRowMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Repository
public class JdbcGebruikerDAO implements GebruikerDAO{
    private final Logger logger = LoggerFactory.getLogger(JdbcGebruikerDAO.class);

    private JdbcTemplate jdbcTemplate;

    @Autowired
    public JdbcGebruikerDAO (JdbcTemplate jdbcTemplate) {
        super();
        this.jdbcTemplate = jdbcTemplate;
        logger.info("New JdbcGebruikerDAO");
    }

    private PreparedStatement insertGebruikerStatement (Gebruiker gebruiker, Connection connection) throws SQLException {
        String sql = "insert into codelab_jdbctemplate.gebruiker(voornaam, achternaam, gebdatum, email) values (?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setString(1,gebruiker.getVoornaam());
        preparedStatement.setString(2, gebruiker.getAchternaam());
        preparedStatement.setString(4, gebruiker.getEmail());

        try {
            preparedStatement.setDate(3, new java.sql.Date(
                    new SimpleDateFormat("dd-MM-yyyy").parse(formatDateForSqlStatement(gebruiker.getGeboortedatum())).getTime()));
        } catch (ParseException pe) {}
        return preparedStatement;
    }

    private String formatDateForSqlStatement(Date date) {
        try {
            String dateString = date.toString();
            DateFormat originalFormat = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
            DateFormat outputFormat = new SimpleDateFormat("dd-MM-yyyy");
            return outputFormat.format(originalFormat.parse(dateString));
        } catch (ParseException pe) {}
        return "";
    }

    @Override
    public List<Gebruiker> listGebruikers() {
        String sql = "select * from codelab_jdbctemplate.gebruiker";
        return jdbcTemplate.query(sql, (rs, rwNr) -> new Gebruiker(rs.getLong("id"),
                rs.getString("voornaam"), rs.getString("achternaam"), rs.getDate("gebdatum"), rs.getString("email")));
    }

    @Override
    public Gebruiker findById(long id){
        String sql = "select * from codelab_jdbctemplate.gebruiker where id =?";
        List<Gebruiker> lijstGebruikers = jdbcTemplate.query(sql, new GebruikerRowMapper(), id);
        return lijstGebruikers.size() == 1 ? lijstGebruikers.get(0) : null;
    }

    @Override
    public Gebruiker findByEmail(String email) {
        String sql = "select * from codelab_jdbctemplate.gebruiker where email =?";
        List<Gebruiker> lijstGebruikers = jdbcTemplate.query(sql, new GebruikerRowMapper(), email);
        return lijstGebruikers.size() == 1 ? lijstGebruikers.get(0) : null;
    }

    @Override
    public Gebruiker findaByFullName(String voornaam, String achternaam) {
        String sql = "select * from codelab_jdbctemplate.gebruiker where voornaam =? AND achternaam =?";
        List<Gebruiker> lijstGebruikers = jdbcTemplate.query(sql, new GebruikerRowMapper(), voornaam, achternaam);
        return lijstGebruikers.size() == 1 ? lijstGebruikers.get(0) : null;
    }

    @Override
    public int saveOne(Gebruiker gebruiker) {
        logger.info("GebruikerDAO.saveOne aangeroepen");
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> insertGebruikerStatement(gebruiker, connection), keyHolder);
        int newKey = keyHolder.getKey().intValue();
        gebruiker.setId((long)newKey);
        return newKey;
    }

    @Override
    public void updateOne(Gebruiker gebruiker) {
        logger.info("GebruikerDAO.updateOne aangeroepen");
        String sql = "update codelab_jdbctemplate.gebruiker set voornaam = ?, achternaam = ?, gebdatum = ? , email = ?" +
                "where id = ?";
        Gebruiker g1 = findById(gebruiker.getId());

        if (g1 != null) {
            jdbcTemplate.update(sql, gebruiker.getVoornaam(), gebruiker.getAchternaam(), gebruiker.getGeboortedatum(),
                gebruiker.getEmail(), gebruiker.getId());
        } else {
            saveOne(gebruiker);
        }
    }

    @Override
    public void deleteOne(Gebruiker gebruiker) {
        logger.info("GebruikerDAO.deleteOne aangeroepen");
        Gebruiker g1 = findById(gebruiker.getId());
        String sql = "delete from codelab_jdbctemplate.gebruiker where id = ?";

        if (g1 != null) {
            jdbcTemplate.update(sql, gebruiker.getId());
        } else return;
    }
}
