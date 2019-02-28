package com.gkwang.sqlload.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 文件操作工具类
 * 
 * @Title: FileUtil.java
 * @Package:com.gkwang.sqlload.util
 * @author:Wanggk
 * @date:2018年10月19日
 * @version:V1.0
 */
@Component()
public class FileServer {
	public String originalPath = "D:/hundsun-t/odata";
	public String goalPath = "D:/hundsun-t/data";
	public String controlPath = "D:/hundsun-t/control";
	public String logPath = "D:/hundsun-t/log";
	public String historyPath = "D:/hundsun-t/History.txt";
	public String idPath = "D:/hundsun-t/id.txt";
	@Autowired
	private ConvertServer convertUtil;

	/**
	 * 获取扫描历史内容
	 * 
	 * @param:@param historyPath
	 * @param:@return
	 * @param:@throws Exception
	 * @return:List<String>
	 * @author:wanggk
	 * @date:2018年11月1日
	 * @version:V1.0
	 */
	public List<String> getHistory(String historyPath) throws Exception {
		List<String> histories = new ArrayList<>();
		InputStreamReader isr = new InputStreamReader(new FileInputStream(historyPath), "GBK");
		BufferedReader bufferedReader = new BufferedReader(isr);
		String history = null;
		while ((history = bufferedReader.readLine()) != null) {
			histories.add(history);
		}
		bufferedReader.close();
		isr.close();
		return histories;
	}

	/**
	 * 获取给定目录下所有文件路径
	 * 
	 * @param:@param Ppath
	 * @param:@return
	 * @return:Map<Integer,String>
	 * @author:wanggk
	 * @date:2018年10月19日
	 * @version:V1.0
	 * @throws Exception
	 */
	public Map<Integer, String> getFiles(String Ppath, String historyPath, String idPath) throws Exception {
		Map<Integer, String> filemap = new HashMap<>();
		List<String> histories = getHistory(historyPath);
		File Pfile = new File(Ppath);
		File[] PtempList = Pfile.listFiles();
		OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(historyPath, true), "GBK");
		BufferedWriter bufferedWriter = new BufferedWriter(fw);
		InputStreamReader isr = new InputStreamReader(new FileInputStream(idPath), "UTF-8");
		BufferedReader bufferedReader = new BufferedReader(isr);
		String idline = bufferedReader.readLine();
		Integer id = Integer.valueOf(idline);
		for (int i = 0; i < PtempList.length; i++) {
			String Cpath = PtempList[i].toString();
			File CFile = new File(Cpath);
			File[] CtempList = CFile.listFiles();
			for (int j = 0; j < CtempList.length; j++) {
				String ICpath = CtempList[j].toString();
				File ICFile = new File(ICpath);
				File[] ICtempList = ICFile.listFiles();
				for (int p = 0; p < ICtempList.length; p++) {
					File file = new File(ICtempList[p].toString());
					int flag = 0;
					for (int k = 0; k < histories.size(); k++) {
						String history = histories.get(k);
						String[] split = history.split(",");
						if (ICtempList[p].toString().equals(split[1])) {
							if (file.lastModified() == Long.valueOf(split[2])
									&& file.length() == Long.valueOf(split[3])) {
								flag = 1;
							} else {
								flag = 2;
							}
						}

					}
					if (flag == 0) {
						filemap.put(id, ICtempList[p].toString());
						bufferedWriter.write(
								id + "," + ICtempList[p].toString() + "," + file.lastModified() + "," + file.length());
						bufferedWriter.newLine();
						bufferedWriter.flush();
						id++;
					} else if (flag == 2) {
						filemap.put(-1, ICtempList[p].toString());
						bufferedWriter.write(
								"变更," + ICtempList[p].toString() + "," + file.lastModified() + "," + file.length());
						bufferedWriter.newLine();
						bufferedWriter.flush();

					}
				}

			}
		}
		bufferedReader.close();
		bufferedWriter.close();
		isr.close();
		fw.close();
		// 更新最新
		OutputStreamWriter fWriter = new OutputStreamWriter(new FileOutputStream(idPath), "UTF-8");
		BufferedWriter bufferedWriter1 = new BufferedWriter(fWriter);
		bufferedWriter1.write(id.toString());
		bufferedWriter1.close();
		fWriter.close();
		return filemap;
	}

	/**
	 * 将所有文件路径进行分解并判断，如果不是txt文件则转换为txt文件
	 * 
	 * @param:@param filemap
	 * @return:void
	 * @author:wanggk
	 * @date:2018年10月19日
	 * @version:V1.0
	 * @param id
	 * @throws IOException
	 */
	@Async
	public String convertToTxt(String path, String newpath) throws IOException {

		// 将转换成功的文件添加到filespath中，以便生成控制文件
		// List<String> filespath = new ArrayList<>();
		// String newfilepath = "";
		// 将路径分解
		String[] singlepaths = path.split("\\\\");
		String filename = singlepaths[singlepaths.length - 1];
		String datetime = singlepaths[singlepaths.length - 2];
		String ZTBH = singlepaths[singlepaths.length - 3];
		if (!filename.contains(".")) {
			filename = filename + ".unknown";
		}
		String[] filenames = filename.split("\\.");
		String suffixname = filenames[1];
		String allnewpath = null;
		// 开始判断并转换
		if (suffixname.equals("txt")) {
			allnewpath = newpath + "\\" + ZTBH + "\\" + datetime + "\\" + filename;
			convertUtil.TxtToTxt(path, allnewpath, datetime, ZTBH);
		} else if (suffixname.equals("xls")) {
			allnewpath = newpath + "\\" + ZTBH + "\\" + datetime + "\\" + filenames[0] + ".txt";
			convertUtil.ExcelToTxt(path, allnewpath, datetime, ZTBH);
		} else if (suffixname.equals("dbf") || suffixname.equals("DBF")) {
			allnewpath = newpath + "\\" + ZTBH + "\\" + datetime + "\\" + filenames[0] + ".txt";
			convertUtil.DbfToTxt(path, allnewpath, datetime, ZTBH);
		}
		return allnewpath;
	}

	/**
	 * 复制目录
	 * 
	 * @param:
	 * @return:void
	 * @author:wanggk
	 * @date:2018年10月19日
	 * @version:V1.0
	 */
	public void CopyDir(String oriPath, String goalPath, String controlPath, String logPath) {
		File orifile = new File(oriPath);
		File[] oridir = orifile.listFiles();
		for (int i = 0; i < oridir.length; i++) {
			String path = oridir[i].toString();
			String[] one = path.split("\\\\");
			// dirname.add(one[one.length - 1]);
			File file = new File(path);
			File[] listFiles = file.listFiles();
			for (int j = 0; j < listFiles.length; j++) {
				String datepath = listFiles[j].toString();
				String[] two = datepath.split("\\\\");
				String goal = goalPath + "/" + one[one.length - 1] + "/" + two[two.length - 1];
				String control = controlPath + "/" + one[one.length - 1] + "/" + two[two.length - 1];
				String log = logPath + "/" + one[one.length - 1] + "/" + two[two.length - 1];
				File goalfile = new File(goal);
				File controlfile = new File(control);
				File logfile = new File(log);
				if (!goalfile.exists()) {
					goalfile.mkdirs();
				}
				if (!controlfile.exists()) {
					controlfile.mkdirs();
				}
				if (!logfile.exists()) {
					logfile.mkdirs();
				}
			}
		}

	}

	/**
	 * 生成控制文件
	 * 
	 * @param:@param controlPath
	 * @param:@param paths
	 * @return:void
	 * @author:wanggk
	 * @date:2018年10月22日
	 * @version:V1.0
	 * @throws IOException
	 */
	@Async
	public String createControl(Integer id, String controlPath, String path) throws IOException {
		// List<String> controlpath = new ArrayList<>();

		String[] cpaths = path.split("\\\\");
		// 获取文件名
		String[] filename = cpaths[cpaths.length - 1].split("\\.");
		String fname = filename[0];
		// 替换txt为ctl
		String newpath = controlPath + "/" + cpaths[1] + "/" + cpaths[2] + "/" + fname + ".ctl";
		File controlfile = new File(newpath);
		if (!controlfile.exists()) {
			controlfile.createNewFile();
		}
		FileWriter fWriter = new FileWriter(controlfile);
		BufferedWriter bWriter = new BufferedWriter(fWriter);
		bWriter.write("LOAD DATA CHARACTERSET ZHS16GBK " + " INFILE '" + path + "' APPEND INTO TABLE "
				+ fname.toUpperCase() + " FIELDS TERMINATED BY '|' " + " TRAILING NULLCOLS " + "(");
		List<String> list = getFields(fname.toUpperCase()); // 获取列名
		for (int j = 0; j < list.size(); j++) {
			if (j == list.size() - 1) {
				bWriter.write(list.get(j));
			} else {
				bWriter.write(list.get(j) + ",");
			}
		}
		/**
		 * 	删除变更的旧数据
		 */
		if (id == -1) {
			deleteData(fname.toUpperCase(), cpaths[1], cpaths[2]);
		}
		bWriter.write(")"); // 列名逐个添加到control文件中
		bWriter.flush();
		bWriter.close();
		fWriter.close();
		return newpath;

	}

	/**
	 * 获取数据库中对应表的列名
	 * 
	 * @param:@return
	 * @return:List<String>
	 * @author:wanggk
	 * @date:2018年10月22日
	 * @version:V1.0
	 */

	public List<String> getFields(String tablename) {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		dataSource.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:ORCL");
		dataSource.setUsername("SCAT");
		dataSource.setPassword("scat");
		JdbcTemplate jdbctemplate = new JdbcTemplate();
		jdbctemplate.setDataSource(dataSource);
		String sql = "select column_name from user_tab_columns where table_name='" + tablename + "'";
		List<Map<String, Object>> queryForList = jdbctemplate.queryForList(sql);
		/*List rows = jdbctemplate.queryForList(sql);
		System.out.println(rows);*/
		List<String> fields = new ArrayList<String>();
		for (int i = 0; i < queryForList.size(); i++) {
			fields.add((String) queryForList.get(i).get("COLUMN_NAME"));
		}
		System.out.println(fields);
		return fields;
	}

	/**
	 * 删除有变动的数据
	 * 
	 * @param:@param tablename
	 * @param:@param ZTBH
	 * @param:@param datetime
	 * @return:void
	 * @author:wanggk
	 * @date:2018年11月1日
	 * @version:V1.0
	 */
	public void deleteData(String tablename, String ZTBH, String DATATIME) {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		dataSource.setUrl("jdbc:oracle:thin:@127.0.0.1:1521:ORCL");
		dataSource.setUsername("SCAT");
		dataSource.setPassword("scat");
		JdbcTemplate jdbctemplate = new JdbcTemplate();
		jdbctemplate.setDataSource(dataSource);
		String sql = "delete from " +  tablename  + " where ZTBHA=" + "'" + ZTBH + "'" + "and DATATIME=" + "'"
				+ DATATIME + "'";
		jdbctemplate.execute(sql);
	}

	/**
	 * 执行sqlload命令
	 * 
	 * @param:
	 * @return:void
	 * @author:wanggk
	 * @date:2018年10月22日
	 * @version:V1.0
	 */
	@Async
	public void execute(String control, String logpath) {
		String[] paths = control.split("/");
		String filname = paths[paths.length - 1].replaceAll("ctl", "txt");
		InputStream ins = null;
		String logfile = logpath + "/"+ paths[paths.length - 3] + "/"+ paths[paths.length - 2] + "/" + "log-" + filname;
		String dos = "sqlldr SCAT" + "/scat" + "@orcl" + " control='" + control + "' log='" + logfile + "'";
		String[] cmd = new String[] { "cmd.exe", "/C", dos }; // 命令
		try {
			Process process = Runtime.getRuntime().exec(cmd);
			ins = process.getInputStream(); // 获取执行cmd命令后的信息
			BufferedReader reader = new BufferedReader(new InputStreamReader(ins));
			String line = null;
			while ((line = reader.readLine()) != null) {
				String msg = new String(line.getBytes("ISO-8859-1"), "UTF-8");
				System.out.println(msg); // 输出
			}
			int exitValue = process.waitFor();
			if (exitValue == 0) {
				System.out.println("返回值：" + exitValue + "\n数据导入成功");
			} else {
				System.out.println("返回值：" + exitValue + "\n数据导入失败");

			}

			process.getOutputStream().close(); // 关闭
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	@Scheduled(fixedRate=60*1000)
	public void run() throws Exception {
		// TODO Auto-generated method stub
		System.out.println("run");
		@SuppressWarnings("resource")
		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.register(TaskExecutorConfig.class);
		context.refresh();
		CopyDir(originalPath, goalPath, controlPath, logPath);
		Map<Integer, String> files = getFiles(originalPath, historyPath, idPath);
		Map<Integer, String> list = new HashMap<Integer, String>();
		String control = null;
		String newpath = null;
		for (Map.Entry<Integer, String> entry : files.entrySet()) {
			Integer id = entry.getKey();
			String path = entry.getValue();
			String[] filename = path.split("\\.");
			String suffix = filename[1];
			if (suffix.equals("txt") || suffix.equals("dbf") || suffix.equals("xls") || suffix.equals("DBF")) {
				newpath = convertToTxt(path, goalPath);
			}
			list.put(id, newpath);
		}
		for (Map.Entry<Integer, String> entry : list.entrySet()) {
			Integer id = entry.getKey();
			String path = entry.getValue();
			control = createControl(id, controlPath, path);
			execute(control, logPath);
		}

	}

}
