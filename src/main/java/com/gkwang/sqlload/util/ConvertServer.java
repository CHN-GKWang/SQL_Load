package com.gkwang.sqlload.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import dbf_util.DBFReader;

/**
 * 文件转换工具类
 * 
 * @Title: ConvertUtil.java
 * @Package:com.gkwang.sqlload.util
 * @author:Wanggk
 * @date:2018年10月19日
 * @version:V1.0
 */
@Component
public class ConvertServer {

	/**
	 * Excel文件转换为TXT文件
	 * 
	 * @param:@param oldpath
	 * @param:@param newpath
	 * @return:void
	 * @author:wanggk
	 * @date:2018年10月19日
	 * @version:V1.0
	 */
	public void ExcelToTxt(String oldpath, String newpath, String datetime, String ZTBH) {
		String filepath = oldpath;
		try {
			Workbook workbook = Workbook.getWorkbook(new File(filepath));
			Sheet sheet = workbook.getSheet(0);
			File file = new File(newpath);
			OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(file), "GBK");
			BufferedWriter bw = new BufferedWriter(fw);
			int j = sheet.getRows();
			int y = sheet.getColumns();
			for (int i = 1; i < j; i++) {
				bw.write(ZTBH);
				bw.write("|" + datetime);
				for (int x = 0; x < y; x++) {
					Cell c = sheet.getCell(x, i);
					String s = c.getContents();
					// x != 0 &&
					if (StringUtils.isNotBlank(s))
						bw.write("|" + s);
					/*
					 * else bw.write(s);
					 */
					if (x == y - 1) {
						bw.write("|" + s + "|");
					}
					bw.flush();
				}
				bw.newLine();
				bw.flush();
			}
			bw.close();
			fw.close();
		} catch (BiffException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * DBF文件转换成TXT文件
	 * 
	 * @param:@param oldpath
	 * @param:@param newpath
	 * @return:void
	 * @author:wanggk
	 * @date:2018年10月19日
	 * @version:V1.0
	 * @throws IOException
	 */
	public void DbfToTxt(String oldpath, String newpath, String datetime, String ZTBH) throws IOException {
		File file = new File(newpath);
		if (!file.exists()) {
			file.createNewFile();
		}
		OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(file), "GBK");
		BufferedWriter bw = new BufferedWriter(fw);
		InputStream fis = null;
		try {
			fis = new FileInputStream(oldpath);
			DBFReader reader = new DBFReader(fis);
			reader.setCharactersetName("GBK");
			/*
			 * int fieldsCount = reader.getFieldCount(); for (int i = 0; i < fieldsCount;
			 * i++) { DBFField field = reader.getField(i); }
			 */
			Object[] rowValues;
			while ((rowValues = reader.nextRecord()) != null) {
				bw.write(ZTBH);
				bw.write("|" + datetime + "|");
				for (int i = 0; i < rowValues.length; i++) {
					bw.write(rowValues[i] + "|");
				}
				bw.newLine();
				bw.flush();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				fw.close();
				bw.close();
				fis.close();
			} catch (Exception e) {
			}
		}
	}

	/**
	 * Txt文件根据条件转换为符合导入的Txt文件
	 * 
	 * @param:@param oldpath
	 * @param:@param newpath
	 * @param:@throws IOException
	 * @return:void
	 * @author:wanggk
	 * @date:2018年10月22日
	 * @version:V1.0
	 */
	public void TxtToTxt(String oldpath, String newpath, String datetime, String ZTBH) throws IOException {
		int flag = 0;
		File oldfile = new File(oldpath);
		File newfile = new File(newpath);
		InputStreamReader isr = new InputStreamReader(new FileInputStream(oldfile), "GBK");
		BufferedReader bufferedReader = new BufferedReader(isr);
		OutputStreamWriter fw = new OutputStreamWriter(new FileOutputStream(newfile), "GBK");
		BufferedWriter bufferedWriter = new BufferedWriter(fw);
		String line = bufferedReader.readLine();
		if (line.contains("|")) {
			flag = 1;
		}
		String newline = null;
		if (flag == 1) {
			// .replaceAll("\\|", ",")
			bufferedWriter.write(ZTBH);
			bufferedWriter.write("|" + datetime + "|");
			newline = line.replaceAll(" +", "|");
			if (!newline.endsWith("|")) {
				newline = newline + "|";
			}
			bufferedWriter.write(newline);
			bufferedWriter.newLine();
			bufferedWriter.flush();
			while ((line = bufferedReader.readLine()) != null) {
				// .replaceAll("\\|", ",")
				bufferedWriter.write(ZTBH);
				bufferedWriter.write("|" + datetime + "|");
				newline = line.replaceAll(" +", "|");
				if (!newline.endsWith("|")) {
					newline = newline + "|";
				}
				bufferedWriter.write(newline);
				bufferedWriter.newLine();
				bufferedWriter.flush();
			}
		} else {
			for (int i = 0; i < 3; i++) {
				line = bufferedReader.readLine();
			}
			while ((line = bufferedReader.readLine()) != null) {
				bufferedWriter.write(ZTBH);
				bufferedWriter.write("|" + datetime + "|");
				newline = line.replaceAll(" +", "|");
				if (!newline.endsWith("|")) {
					newline = newline + "|";
				}
				bufferedWriter.write(newline);
				bufferedWriter.newLine();
				bufferedWriter.flush();
			}
		}
		bufferedReader.close();
		bufferedWriter.close();
		isr.close();
		fw.close();
	}
	public void go() {
		System.out.println("不为空");
	}

}