package model.dao.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;

public class SellerDaoJDBC<K, V> implements SellerDao {

	private Connection conn;

	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller obj) {
		PreparedStatement st = null;
		try {// comando sql:
			st = conn.prepareStatement("INSERT INTO seller " + "(Name, Email, BirthDate, BaseSalary, DepartmentId) "
					+ "VALUES " + "(?, ?, ?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
			// setando valores
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			// executando o comando sql:
			int rowsAffected = st.executeUpdate();

			// testar se foi instanciando um novo seller e pegar esse id:
			if (rowsAffected > 0) {
				ResultSet rs = st.getGeneratedKeys();
				// adicionar esse id(st.getGeneratedKeys()) a uma variavel e lançar na primeira
				// coluna do Seller obj:
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);

				} else {
					throw new DbException("Unexpected error ocurred! No rows Affecteds");
				}
			}
			DB.closeResultSet(null);

		} 
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void update(Seller obj) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("UPDATE seller "
					+"SET Name = ?, Email = ?, BirthDate = ?, BaseSalary = ?, DepartmentId = ? "
					+"WHERE Id = ?");
			st.setString(1, obj.getName());
			st.setString(2, obj.getEmail());
			st.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			st.setDouble(4, obj.getBaseSalary());
			st.setInt(5, obj.getDepartment().getId());
			st.setInt(6, obj.getId());
			
			st.executeUpdate();
		}
		catch(SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public void deleteById(Integer id) {
		PreparedStatement st = null;
		try {
			st = conn.prepareStatement(
					"DELETE FROM seller WHERE Id = ?");
			st.setInt(1, id);
			
		}
		catch(SQLException e){
			throw new DbException(e.getMessage());			
		}
		finally {
			DB.closeStatement(st);
		}
	}

	@Override
	public Seller findById(Integer id) {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT seller.*, department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "WHERE seller.Id = ?");

			st.setInt(1, id);
			rs = st.executeQuery();

			// testar pra ver se veio um seller ou não:
			if (rs.next()) {
				Department dep = instanteDepartment(rs);

				Seller obj = instanteSeller(rs, dep);
				return obj;
			}
			return null;
		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
			DB.closeResultSet(rs);
		}
	}

	private Seller instanteSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller();
		obj.setId(rs.getInt("Id"));
		obj.setName(rs.getString("Name"));
		obj.setEmail(rs.getString("Email"));
		obj.setBaseSalary(rs.getDouble("BaseSalary"));
		obj.setBirthDate(rs.getDate("BirthDate"));
		obj.setDepartment(dep);
		return obj;
	}

	private Department instanteDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department();
		dep.setId(rs.getInt("DepartmentId"));
		dep.setName(rs.getString("DepName"));
		return dep;
	}

	@Override
	public List<Seller> findAll() {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "ORDER BY Name");
			rs = st.executeQuery();

			List<Seller> list = new ArrayList<Seller>();

			// criar um map vazio:
			Map<Integer, Department> map = new HashMap<>();

			while (rs.next()) {
				// testar se o dep ja existe, indo no map e buscando um dep que tenha o id
				// stringado, se nao existe o map.get retorna null
				Department dep = map.get(rs.getInt("DepartmentId"));

				if (dep == null) {
					// caso o dep acima seja nulo: instancio o dep e guardo no map
					dep = instanteDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				Seller obj = instanteSeller(rs, dep);
				list.add(obj);
			}
			return list;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
		}

	}

	@Override
	public List<Seller> findByDepartment(Department department) {
		PreparedStatement st = null;
		ResultSet rs = null;
		try {
			st = conn.prepareStatement(
					"SELECT seller.*,department.Name as DepName " + "FROM seller INNER JOIN department "
							+ "ON seller.DepartmentId = department.Id " + "WHERE DepartmentId = ? " + "ORDER BY Name");

			st.setInt(1, department.getId());
			rs = st.executeQuery();

			List<Seller> list = new ArrayList<Seller>();

			// criar um map vazio:
			Map<Integer, Department> map = new HashMap<>();

			while (rs.next()) {
				// testar se o dep ja existe, indo no map e buscando um dep que tenha o id
				// stringado, se nao existe o map.get retorna null
				Department dep = map.get(rs.getInt("DepartmentId"));

				if (dep == null) {
					// caso o dep acima seja nulo: instancio o dep e guardo no map
					dep = instanteDepartment(rs);
					map.put(rs.getInt("DepartmentId"), dep);
				}
				Seller obj = instanteSeller(rs, dep);
				list.add(obj);
			}
			return list;

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
		}
	}
}
