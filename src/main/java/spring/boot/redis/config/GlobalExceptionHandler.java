package spring.boot.redis.config;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.servlet.ModelAndView;

import spring.boot.redis.entity.ErrorInfo;

@ControllerAdvice
public class GlobalExceptionHandler {

	public static final String DEFAULT_ERROR_VIEW = "error";
	
	@ExceptionHandler(value = Exception.class)
	public ModelAndView defaultErrorView(HttpServletRequest req, Exception e) throws Exception{
		ModelAndView mav = new ModelAndView();
		mav.addObject("exception", e);
		mav.addObject("url", req.getRequestURL());
		mav.setViewName(DEFAULT_ERROR_VIEW);
		return mav;
	}
	
	@ResponseBody
	@ExceptionHandler(value=MyException.class)
	public ErrorInfo<String> defaultErrorView1(HttpServletRequest req, MyException e) throws Exception{
		ErrorInfo<String> error = new ErrorInfo<String>();
		error.setCode(ErrorInfo.ERROR);
		error.setMessage(e.getMessage());
		//error.setData("1+1");
		error.setUrl(req.getRequestURL().toString());
		return error;
	}
}
