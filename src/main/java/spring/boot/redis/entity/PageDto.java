package spring.boot.redis.entity;

public class PageDto<T> {

	private String restCode;
	private String restMsg;
	private T data;
	private PageData pageData;

	public String getRestCode() {
		return restCode;
	}

	public void setRestCode(String restCode) {
		this.restCode = restCode;
	}

	public String getRestMsg() {
		return restMsg;
	}

	public void setRestMsg(String restMsg) {
		this.restMsg = restMsg;
	}

	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
	}

	public PageData getPageData() {
		return pageData;
	}

	public void setPageData(PageData pageData) {
		this.pageData = pageData;
	}

}
