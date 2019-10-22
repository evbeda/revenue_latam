def query_info(request):
    return {'query_info': request.session.get('query_info')}
