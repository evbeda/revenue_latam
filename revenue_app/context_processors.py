def context(request):
    return {
        'query_info': request.session.get('query_info'),
        'exchange_data': request.session.get('exchange_data'),
        'class_exchange': request.session.get('class_exchange'),
    }
