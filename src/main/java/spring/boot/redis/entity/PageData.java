/**
 * 
 */
package spring.boot.redis.entity;

/**
 * @author 080806310
 * @date: 2018年5月22日 下午9:13:16 
 */
public class PageData {
	
	private Long pageTotal;
	
	private int pages;

    private int pageSize;

	public PageData() {
	}

	public PageData(Long pageTotal, int pages, int pageSize) {
		super();
		this.pageTotal = pageTotal;
		this.pages = pages;
		this.pageSize = pageSize;
	}

	public Long getPageTotal() {
		return pageTotal;
	}

	public void setPageTotal(Long pageTotal) {
		this.pageTotal = pageTotal;
	}

	public int getPages() {
		return pages;
	}

	public void setPages(int pages) {
		this.pages = pages;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	
}
